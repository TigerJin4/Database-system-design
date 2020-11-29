package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;
        private LeftRightComparator comparator;

        private SortMergeIterator() {
            super();
            SortOperator left = new SortOperator(SortMergeOperator.this.getTransaction(), getLeftTableName(), new LeftRecordComparator());
            SortOperator right = new SortOperator(SortMergeOperator.this.getTransaction(), getRightTableName(), new RightRecordComparator());

            leftIterator = SortMergeOperator.this.getRecordIterator(left.sort());
            rightIterator = SortMergeOperator.this.getRecordIterator(right.sort());
            nextRecord = null;
            comparator = new LeftRightComparator();

            if (leftIterator.hasNext()) {
                leftRecord = leftIterator.next();
                marked = false;
            } if (rightIterator.hasNext()) {
                rightRecord = rightIterator.next();
                marked = false;
            } else {
                rightRecord = null;
                leftRecord = null;
                marked = false;
            }
            try {
                fetchNextRecord();
            } catch (NoSuchElementException err) {
                nextRecord = null;
            }
            // TODO(proj3_part1): implement
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement

//            return false;
            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Record result = nextRecord;
            try {
                fetchNextRecord();
            } catch (NoSuchElementException err) {
                nextRecord = null;
            }
            return result;
//            throw new NoSuchElementException();
        }

        /**
         * Fetch the next record. It will assign the next record to this.nextrecord
         */

        public void fetchNextRecord() {
            if (leftRecord == null) {
                throw new NoSuchElementException("No new record to fetch");
            }
            nextRecord = null;
            while (!hasNext()) {
                if (leftRecord == null) throw new NoSuchElementException("No record");
                if (!marked) {
                    // follow the psuedocode
                    while (comparator.compare(leftRecord, rightRecord) < 0) {
                        leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    }
                    while (comparator.compare(leftRecord, rightRecord) > 0) {
                        rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    }
                    rightIterator.markPrev();
                    marked = true;
                }
                if (leftRecord != null && rightRecord != null && comparator.compare(leftRecord, rightRecord) == 0) {
                    List<DataBox> left = new ArrayList<>(leftRecord.getValues());
                    List<DataBox> right = new ArrayList<>(rightRecord.getValues());
                    left.addAll(right);
                    nextRecord = new Record(left);
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                }

                else {
                    leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    rightIterator.reset();
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    marked = false;
                }
            }
        }
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }

        /**
         * Left-Right Record comparator
         * o1: leftRecord o: rightRecord
         */
        private class LeftRightComparator implements Comparator<Record> {
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
