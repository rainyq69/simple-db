package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Catalog;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private HeapFile file;
    private TransactionId transactionId;
    private int pageIndex;
    private HeapPage curPage;
    int numPages;
    boolean opened = false;
    private Iterator<Tuple> tupleIterator;

    public HeapFileIterator(HeapFile file, TransactionId transactionId) {
        this.file = file;
        this.transactionId = transactionId;
    }

    /**
     * Opens the iterator
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        opened = true;
        pageIndex = 0;
        numPages = file.numPages();
        curPage = (HeapPage) file.readPage(new HeapPageId(file.getId(), pageIndex));
        tupleIterator = curPage.iterator();
    }

    /**
     * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!opened) return false;
        if (tupleIterator.hasNext()) return true;
        else if (pageIndex != numPages - 1) {
            pageIndex++;
            curPage = (HeapPage) file.readPage(new HeapPageId(file.getId(), pageIndex));
            tupleIterator = curPage.iterator();
            return tupleIterator.hasNext();
        } else return false;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext()) throw new NoSuchElementException();
        else return tupleIterator.next();
    }

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Closes the iterator.
     */
    @Override
    public void close() {
        opened = false;
    }
}
