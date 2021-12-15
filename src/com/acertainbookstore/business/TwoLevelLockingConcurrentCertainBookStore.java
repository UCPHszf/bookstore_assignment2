package com.acertainbookstore.business;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreConstants;
import com.acertainbookstore.utils.BookStoreException;
import com.acertainbookstore.utils.BookStoreUtility;

/**
 * {@link TwoLevelLockingConcurrentCertainBookStore} implements the {@link BookStore} and
 * {@link StockManager} functionalities.
 *
 * @see BookStore
 * @see StockManager
 */
public class TwoLevelLockingConcurrentCertainBookStore implements BookStore, StockManager {

    /**
     * The mapping of books from ISBN to {@link BookStoreBook}.
     */
    private Map<Integer, BookStoreBook> bookMap = null;
    private Map<Integer, ReentrantReadWriteLock> lockMap;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock topLevelReadLock = readWriteLock.readLock();
    private final Lock topLevelWriteLock = readWriteLock.writeLock();

    /**
     * Instantiates a new {@link TwoLevelLockingConcurrentCertainBookStore}.
     */
    public TwoLevelLockingConcurrentCertainBookStore() {
        // Constructors are not synchronized
        bookMap = new HashMap<>();
        lockMap = new ConcurrentHashMap<>();
    }

    private void checkISBN(final int isbn) throws BookStoreException {
        if (!lockMap.containsKey(isbn)) {
            throw new BookStoreException("ISBN" + isbn + BookStoreConstants.NOT_AVAILABLE);
        }
    }

    private void acquireBottomLevelReadLock(Integer isbn) throws BookStoreException {
        checkISBN(isbn);
        ReadWriteLock lock = lockMap.get(isbn);
        lock.readLock().lock();
    }

    private void acquireBottomLevelReadLock(Iterable<Integer> isbns) {
        isbns.forEach(isbn -> {
            try {
                acquireBottomLevelReadLock(isbn);
            } catch (BookStoreException e) {
                e.printStackTrace();
            }
        });
    }

    private void acquireBottomLevelWriteLock(Integer isbn) throws BookStoreException {
        checkISBN(isbn);
        ReadWriteLock lock = lockMap.get(isbn);
        lock.writeLock().lock();
    }

    private void releaseBottomLevelWriteLockByISBN(Integer isbn) {
        if (lockMap.containsKey(isbn)) {
            try {
                lockMap.get(isbn).writeLock().unlock();
            } catch (IllegalMonitorStateException ex) {
                System.out.println("Warning: " + Thread.currentThread().getName() + " attempted to unlock a write lock it did not hold.");
            }
        }
    }

    private void releaseBottomLevelWriteLockByISBN(Iterable<Integer> isbns) {
        isbns.forEach(this::releaseBottomLevelWriteLockByISBN);
    }

    private void releaseBottomLevelReadLockByISBN(Integer isbn) {
        if (lockMap.containsKey(isbn)) {
            try {
                lockMap.get(isbn).readLock().unlock();
            } catch (IllegalMonitorStateException ex) {
                System.out.println("Warning: " + Thread.currentThread().getName() + " attempted to unlock a read lock it did not hold.");
            }
        }
    }

    private void releaseBottomLevelReadLockByISBN(Iterable<Integer> isbns) {
        isbns.forEach(this::releaseBottomLevelReadLockByISBN);
    }

    private void validate(StockBook book) throws BookStoreException {
        int isbn = book.getISBN();
        String bookTitle = book.getTitle();
        String bookAuthor = book.getAuthor();
        int noCopies = book.getNumCopies();
        float bookPrice = book.getPrice();

        if (BookStoreUtility.isInvalidISBN(isbn)) { // Check if the book has valid ISBN
            throw new BookStoreException(BookStoreConstants.ISBN + isbn + BookStoreConstants.INVALID);
        }

        if (BookStoreUtility.isEmpty(bookTitle)) { // Check if the book has valid title
            throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
        }

        if (BookStoreUtility.isEmpty(bookAuthor)) { // Check if the book has valid author
            throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
        }

        if (BookStoreUtility.isInvalidNoCopies(noCopies)) { // Check if the book has at least one copy
            throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
        }

        if (bookPrice < 0.0) { // Check if the price of the book is valid
            throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
        }

        if (bookMap.containsKey(isbn)) {// Check if the book is not in stock
            throw new BookStoreException(BookStoreConstants.ISBN + isbn + BookStoreConstants.DUPLICATED);
        }
    }

    private void validate(BookCopy bookCopy) throws BookStoreException {
        int isbn = bookCopy.getISBN();
        int numCopies = bookCopy.getNumCopies();

        validateISBNInStock(isbn); // Check if the book has valid ISBN and in stock

        if (BookStoreUtility.isInvalidNoCopies(numCopies)) { // Check if the number of the book copy is larger than zero
            throw new BookStoreException(BookStoreConstants.NUM_COPIES + numCopies + BookStoreConstants.INVALID);
        }
    }

    private void validate(BookEditorPick editorPickArg) throws BookStoreException {
        int isbn = editorPickArg.getISBN();
        validateISBNInStock(isbn); // Check if the book has valid ISBN and in stock
    }

    private void validateISBNInStock(Integer ISBN) throws BookStoreException {
        if (BookStoreUtility.isInvalidISBN(ISBN)) { // Check if the book has valid ISBN
            throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.INVALID);
        }
        if (!bookMap.containsKey(ISBN)) {// Check if the book is in stock
            throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.NOT_AVAILABLE);
        }
    }

    private void validate(BookRating rating) throws BookStoreException {
        if (rating.getRating() < 0 || rating.getRating() > 5) {
            throw new BookStoreException(BookStoreConstants.RATING + rating + BookStoreConstants.INVALID);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.acertainbookstore.interfaces.StockManager#addBooks(java.util.Set)
     */
    public void addBooks(Set<StockBook> bookSet) throws BookStoreException {
        if (bookSet == null) {
            throw new BookStoreException(BookStoreConstants.NULL_INPUT);
        }
        topLevelWriteLock.lock();
        try {
            // Check if all are there
            for (StockBook book : bookSet) {
                validate(book);
            }
            for (StockBook book : bookSet) {
                int isbn = book.getISBN();
                bookMap.put(isbn, new BookStoreBook(book));
                lockMap.put(isbn, new ReentrantReadWriteLock());
            }
        } finally {
            topLevelWriteLock.unlock();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.acertainbookstore.interfaces.StockManager#addCopies(java.util.Set)
     */
    public void addCopies(Set<BookCopy> bookCopiesSet) throws BookStoreException {
        int isbn;
        int numCopies;

        if (bookCopiesSet == null) {
            throw new BookStoreException(BookStoreConstants.NULL_INPUT);
        }

        topLevelReadLock.lock();
        try {
            for (BookCopy bookCopy : bookCopiesSet) {
                acquireBottomLevelWriteLock(bookCopy.getISBN());
                validate(bookCopy);
            }

            BookStoreBook book;

            // Update the number of copies
            for (BookCopy bookCopy : bookCopiesSet) {
                isbn = bookCopy.getISBN();
                numCopies = bookCopy.getNumCopies();
                book = bookMap.get(isbn);
                book.addCopies(numCopies);
            }
        } finally {
            releaseBottomLevelWriteLockByISBN(bookCopiesSet.stream().map(BookCopy::getISBN).collect(Collectors.toList()));
            topLevelReadLock.unlock();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.StockManager#getBooks()
     */
    public List<StockBook> getBooks() {

        List<StockBook> res;

        topLevelReadLock.lock();
        try {
            acquireBottomLevelReadLock(new ArrayList<>(bookMap.keySet()));
            res = bookMap.values().stream()
                    .map(book -> book.immutableStockBook())
                    .collect(Collectors.toList());

        } finally {
            releaseBottomLevelReadLockByISBN(new ArrayList<>(bookMap.keySet()));
            topLevelReadLock.unlock();
        }
        return res;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.acertainbookstore.interfaces.StockManager#updateEditorPicks(java.util
     * .Set)
     */
    public void updateEditorPicks(Set<BookEditorPick> editorPicks) throws BookStoreException {
        // Check that all ISBNs that we add/remove are there first.
        if (editorPicks == null) {
            throw new BookStoreException(BookStoreConstants.NULL_INPUT);
        }

        topLevelReadLock.lock();

        try {

            for (BookEditorPick editorPickArg : editorPicks) {
                acquireBottomLevelWriteLock(editorPickArg.getISBN());
                validate(editorPickArg);
            }

            for (BookEditorPick editorPickArg : editorPicks) {
                bookMap.get(editorPickArg.getISBN()).setEditorPick(editorPickArg.isEditorPick());
            }
        } finally {
            releaseBottomLevelWriteLockByISBN(editorPicks.stream().map(BookEditorPick::getISBN).collect(Collectors.toList()));
            topLevelReadLock.unlock();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.BookStore#buyBooks(java.util.Set)
     */
    public void buyBooks(Set<BookCopy> bookCopiesToBuy) throws BookStoreException {
        if (bookCopiesToBuy == null) {
            throw new BookStoreException(BookStoreConstants.NULL_INPUT);
        }

        // Check that all ISBNs that we buy are there first.
        int isbn;
        BookStoreBook book;
        Boolean saleMiss = false;

        Map<Integer, Integer> salesMisses = new HashMap<>();

        topLevelReadLock.lock();
        try {
            for (BookCopy bookCopyToBuy : bookCopiesToBuy) {
                isbn = bookCopyToBuy.getISBN();
                acquireBottomLevelWriteLock(isbn);
                validate(bookCopyToBuy);

                book = bookMap.get(isbn);

                if (!book.areCopiesInStore(bookCopyToBuy.getNumCopies())) {
                    // If we cannot sell the copies of the book, it is a miss.
                    salesMisses.put(isbn, bookCopyToBuy.getNumCopies() - book.getNumCopies());
                    saleMiss = true;
                }
            }

            // We throw exception now since we want to see how many books in the
            // order incurred misses which is used by books in demand
            if (saleMiss) {
                for (Map.Entry<Integer, Integer> saleMissEntry : salesMisses.entrySet()) {
                    book = bookMap.get(saleMissEntry.getKey());
                    book.addSaleMiss(saleMissEntry.getValue());
                }
                throw new BookStoreException(BookStoreConstants.BOOK + BookStoreConstants.NOT_AVAILABLE);
            }

            // Then make the purchase.
            for (BookCopy bookCopyToBuy : bookCopiesToBuy) {
                book = bookMap.get(bookCopyToBuy.getISBN());
                book.buyCopies(bookCopyToBuy.getNumCopies());
            }
        } finally {
            releaseBottomLevelWriteLockByISBN(bookCopiesToBuy.stream().map(BookCopy::getISBN).collect(Collectors.toList()));
            topLevelReadLock.unlock();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.acertainbookstore.interfaces.StockManager#getBooksByISBN(java.util.
     * Set)
     */
    public List<StockBook> getBooksByISBN(Set<Integer> isbnSet) throws BookStoreException {
        if (isbnSet == null) {
            throw new BookStoreException(BookStoreConstants.NULL_INPUT);
        }
        List<StockBook> res;
        topLevelReadLock.lock();
        try {
            for (Integer ISBN : isbnSet) {
                acquireBottomLevelReadLock(ISBN);
                validateISBNInStock(ISBN);
            }
            res = isbnSet.stream()
                    .map(isbn -> bookMap.get(isbn).immutableStockBook())
                    .collect(Collectors.toList());
        } finally {
            releaseBottomLevelReadLockByISBN(isbnSet);
            topLevelReadLock.unlock();
        }
        return res;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.BookStore#getBooks(java.util.Set)
     */
    public List<Book> getBooks(Set<Integer> isbnSet) throws BookStoreException {
        if (isbnSet == null) {
            throw new BookStoreException(BookStoreConstants.NULL_INPUT);
        }

        topLevelReadLock.lock();
        List<Book> res;
        try {
            // Check that all ISBNs that we rate are there to start with.
            for (Integer ISBN : isbnSet) {
                acquireBottomLevelReadLock(ISBN);
                validateISBNInStock(ISBN);
            }
            res = isbnSet.stream()
                    .map(isbn -> bookMap.get(isbn).immutableBook())
                    .collect(Collectors.toList());
        } finally {
            releaseBottomLevelReadLockByISBN(isbnSet);
            topLevelReadLock.unlock();
        }
        return res;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.BookStore#getEditorPicks(int)
     */
    public List<Book> getEditorPicks(int numBooks) throws BookStoreException {
        if (numBooks < 0) {
            throw new BookStoreException("numBooks = " + numBooks + ", but it must be positive");
        }
        topLevelReadLock.lock();
        acquireBottomLevelReadLock(bookMap.keySet());
        List<BookStoreBook> listAllEditorPicks;
        try {
            listAllEditorPicks = bookMap.entrySet().stream()
                    .map(pair -> pair.getValue())
                    .filter(book -> book.isEditorPick())
                    .collect(Collectors.toList());
        } finally {
            releaseBottomLevelReadLockByISBN(bookMap.keySet());
            topLevelReadLock.unlock();
        }
        // Find numBooks random indices of books that will be picked.
        Random rand = new Random();
        Set<Integer> tobePicked = new HashSet<>();
        int rangePicks = listAllEditorPicks.size();

        if (rangePicks <= numBooks) {

            // We need to add all books.
            for (int i = 0; i < listAllEditorPicks.size(); i++) {
                tobePicked.add(i);
            }
        } else {

            // We need to pick randomly the books that need to be returned.
            int randNum;

            while (tobePicked.size() < numBooks) {
                randNum = rand.nextInt(rangePicks);
                tobePicked.add(randNum);
            }
        }

        // Return all the books by the randomly chosen indices.
        return tobePicked.stream()
                .map(index -> listAllEditorPicks.get(index).immutableBook())
                .collect(Collectors.toList());
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.BookStore#getTopRatedBooks(int)
     */
    @Override
    public List<Book> getTopRatedBooks(int numBooks) throws BookStoreException {
        if (numBooks <= 0) throw new BookStoreException(BookStoreConstants.NEGATIVE_PARAMETER);
        List<Book> res;

        topLevelReadLock.lock();
        acquireBottomLevelReadLock(bookMap.keySet());
        try {
            res = bookMap.values().stream()
                    .sorted((book1, book2) -> Float.compare(book2.getAverageRating(), book1.getAverageRating()))
                    .map(BookStoreBook::immutableStockBook)
                    .limit(numBooks)
                    .collect(Collectors.toList());
        } finally {
            releaseBottomLevelReadLockByISBN(bookMap.keySet());
            topLevelReadLock.unlock();
        }
        return res;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.StockManager#getBooksInDemand()
     */
    @Override
    public List<StockBook> getBooksInDemand() throws BookStoreException {
        List<StockBook> res;
        topLevelReadLock.lock();
        acquireBottomLevelReadLock(bookMap.keySet());
        try {
            res = bookMap.values().stream()
                    .filter(book -> book.getNumSaleMisses() > 0)
                    .map(BookStoreBook::immutableStockBook)
                    .collect(Collectors.toList());
        } finally {
            releaseBottomLevelReadLockByISBN(bookMap.keySet());
            topLevelReadLock.unlock();
        }
        return res;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.BookStore#rateBooks(java.util.Set)
     */
    @Override
    public void rateBooks(Set<BookRating> bookRating) throws BookStoreException {
        if (bookRating == null) {
            throw new BookStoreException(BookStoreConstants.NULL_INPUT);
        }

        topLevelReadLock.lock();
        try {
            // Check whether all books are in the collection.
            for (BookRating bookRate : bookRating) {
                acquireBottomLevelWriteLock(bookRate.getISBN());
                validateISBNInStock(bookRate.getISBN());
                validate(bookRate);
            }

            // If all books validated, then perform the ratings (all-or-nothing)
            for (BookRating bookRate : bookRating) {
                BookStoreBook book = bookMap.get(bookRate.getISBN()).copy();
                book.addRating(bookRate.getRating());
            }
        } finally {
            releaseBottomLevelWriteLockByISBN(bookRating.stream().map(BookRating::getISBN).collect(Collectors.toList()));
            topLevelReadLock.unlock();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.StockManager#removeAllBooks()
     */
    public void removeAllBooks() throws BookStoreException {
        bookMap.clear();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.acertainbookstore.interfaces.StockManager#removeBooks(java.util.Set)
     */
    public void removeBooks(Set<Integer> isbnSet) throws BookStoreException {
        if (isbnSet == null) {
            throw new BookStoreException(BookStoreConstants.NULL_INPUT);
        }
        topLevelWriteLock.lock();
        try {
            for (Integer ISBN : isbnSet) {
                if (BookStoreUtility.isInvalidISBN(ISBN)) {
                    throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.INVALID);
                }

                if (!bookMap.containsKey(ISBN)) {
                    throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.NOT_AVAILABLE);
                }
            }

            for (int isbn : isbnSet) {
                bookMap.remove(isbn);
            }
        } finally {
            topLevelWriteLock.unlock();
        }
    }
}
