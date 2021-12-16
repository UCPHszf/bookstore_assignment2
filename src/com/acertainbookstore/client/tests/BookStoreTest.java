package com.acertainbookstore.client.tests;

import static org.junit.Assert.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import com.acertainbookstore.business.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acertainbookstore.client.BookStoreHTTPProxy;
import com.acertainbookstore.client.StockManagerHTTPProxy;
import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreConstants;
import com.acertainbookstore.utils.BookStoreException;

/**
 * {@link BookStoreTest} tests the {@link BookStore} interface.
 *
 * @see BookStore
 */
public class BookStoreTest {

    /**
     * The Constant TEST_ISBN.
     */
    private static final int TEST_ISBN = 3044560;

    /**
     * The Constant NUM_COPIES.
     */
    private static final int NUM_COPIES = 5;

    /**
     * The local test.
     */
    private static boolean localTest = true;

    /**
     * Single lock test
     */
    private static boolean singleLock = false;


    /**
     * The store manager.
     */
    private static StockManager storeManager;

    /**
     * The client.
     */
    private static BookStore client;

    /**
     * Flag for checking if snapshot is wrong
     */
    private static boolean snapshotError = false;

    /**
     * Flag for checking if top-rated book is wrong
     */
    private static boolean totalRatingError = false;

    /**
     * Sets the up before class.
     */
    @BeforeClass
    public static void setUpBeforeClass() {
        try {
            String localTestProperty = System.getProperty(BookStoreConstants.PROPERTY_KEY_LOCAL_TEST);
            localTest = (localTestProperty != null) ? Boolean.parseBoolean(localTestProperty) : localTest;

            String singleLockProperty = System.getProperty(BookStoreConstants.PROPERTY_KEY_SINGLE_LOCK);
            singleLock = (singleLockProperty != null) ? Boolean.parseBoolean(singleLockProperty) : singleLock;

            if (localTest) {
                if (singleLock) {
                    SingleLockConcurrentCertainBookStore store = new SingleLockConcurrentCertainBookStore();
                    storeManager = store;
                    client = store;
                } else {
                    TwoLevelLockingConcurrentCertainBookStore store = new TwoLevelLockingConcurrentCertainBookStore();
                    storeManager = store;
                    client = store;
                }
            } else {
                storeManager = new StockManagerHTTPProxy("http://localhost:8081/stock");
                client = new BookStoreHTTPProxy("http://localhost:8081");
            }

            storeManager.removeAllBooks();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Helper method to add some books.
     *
     * @param isbn   the isbn
     * @param copies the copies
     * @throws BookStoreException the book store exception
     */
    public void addBooks(int isbn, int copies) throws BookStoreException {
        Set<StockBook> booksToAdd = new HashSet<StockBook>();
        StockBook book = new ImmutableStockBook(isbn, "Test of Thrones", "George RR Testin'", (float) 10, copies, 0, 0,
                0, false);
        booksToAdd.add(book);
        storeManager.addBooks(booksToAdd);
    }

    /**
     * Helper method to get the default book used by initializeBooks.
     *
     * @return the default book
     */
    public StockBook getDefaultBook() {
        return new ImmutableStockBook(TEST_ISBN, "Harry Potter and JUnit", "JK Unit", (float) 10, NUM_COPIES, 0, 0, 0,
                false);
    }

    /**
     * Method to add a book, executed before every test case is run.
     *
     * @throws BookStoreException the book store exception
     */
    @Before
    public void initializeBooks() throws BookStoreException {
        Set<StockBook> booksToAdd = new HashSet<StockBook>();
        booksToAdd.add(getDefaultBook());
        storeManager.addBooks(booksToAdd);
    }

    /**
     * Method to clean up the book store, execute after every test case is run.
     *
     * @throws BookStoreException the book store exception
     */
    @After
    public void cleanupBooks() throws BookStoreException {
        storeManager.removeAllBooks();
    }

    /**
     * Tests basic buyBook() functionality.
     *
     * @throws BookStoreException the book store exception
     */
    @Test
    public void testBuyAllCopiesDefaultBook() throws BookStoreException {
        // Set of books to buy
        Set<BookCopy> booksToBuy = new HashSet<BookCopy>();
        booksToBuy.add(new BookCopy(TEST_ISBN, NUM_COPIES));

        // Try to buy books
        client.buyBooks(booksToBuy);

        List<StockBook> listBooks = storeManager.getBooks();
        assertTrue(listBooks.size() == 1);
        StockBook bookInList = listBooks.get(0);
        StockBook addedBook = getDefaultBook();

        assertTrue(bookInList.getISBN() == addedBook.getISBN() && bookInList.getTitle().equals(addedBook.getTitle())
                && bookInList.getAuthor().equals(addedBook.getAuthor()) && bookInList.getPrice() == addedBook.getPrice()
                && bookInList.getNumSaleMisses() == addedBook.getNumSaleMisses()
                && bookInList.getAverageRating() == addedBook.getAverageRating()
                && bookInList.getNumTimesRated() == addedBook.getNumTimesRated()
                && bookInList.getTotalRating() == addedBook.getTotalRating()
                && bookInList.isEditorPick() == addedBook.isEditorPick());
    }

    /**
     * Tests that books with invalid ISBNs cannot be bought.
     *
     * @throws BookStoreException the book store exception
     */
    @Test
    public void testBuyInvalidISBN() throws BookStoreException {
        List<StockBook> booksInStorePreTest = storeManager.getBooks();

        // Try to buy a book with invalid ISBN.
        HashSet<BookCopy> booksToBuy = new HashSet<BookCopy>();
        booksToBuy.add(new BookCopy(TEST_ISBN, 1)); // valid
        booksToBuy.add(new BookCopy(-1, 1)); // invalid

        // Try to buy the books.
        try {
            client.buyBooks(booksToBuy);
            fail();
        } catch (BookStoreException ex) {
            ;
        }

        List<StockBook> booksInStorePostTest = storeManager.getBooks();

        // Check pre and post state are same.
        assertTrue(booksInStorePreTest.containsAll(booksInStorePostTest)
                && booksInStorePreTest.size() == booksInStorePostTest.size());
    }

    /**
     * Tests that books can only be bought if they are in the book store.
     *
     * @throws BookStoreException the book store exception
     */
    @Test
    public void testBuyNonExistingISBN() throws BookStoreException {
        List<StockBook> booksInStorePreTest = storeManager.getBooks();

        // Try to buy a book with ISBN which does not exist.
        HashSet<BookCopy> booksToBuy = new HashSet<BookCopy>();
        booksToBuy.add(new BookCopy(TEST_ISBN, 1)); // valid
        booksToBuy.add(new BookCopy(100000, 10)); // invalid

        // Try to buy the books.
        try {
            client.buyBooks(booksToBuy);
            fail();
        } catch (BookStoreException ex) {
            ;
        }

        List<StockBook> booksInStorePostTest = storeManager.getBooks();

        // Check pre and post state are same.
        assertTrue(booksInStorePreTest.containsAll(booksInStorePostTest)
                && booksInStorePreTest.size() == booksInStorePostTest.size());
    }

    /**
     * Tests that you can't buy more books than there are copies.
     *
     * @throws BookStoreException the book store exception
     */
    @Test
    public void testBuyTooManyBooks() throws BookStoreException {
        List<StockBook> booksInStorePreTest = storeManager.getBooks();

        // Try to buy more copies than there are in store.
        HashSet<BookCopy> booksToBuy = new HashSet<BookCopy>();
        booksToBuy.add(new BookCopy(TEST_ISBN, NUM_COPIES + 1));

        try {
            client.buyBooks(booksToBuy);
            fail();
        } catch (BookStoreException ex) {
            ;
        }

        List<StockBook> booksInStorePostTest = storeManager.getBooks();
        assertTrue(booksInStorePreTest.containsAll(booksInStorePostTest)
                && booksInStorePreTest.size() == booksInStorePostTest.size());
    }

    /**
     * Tests that you can't buy a negative number of books.
     *
     * @throws BookStoreException the book store exception
     */
    @Test
    public void testBuyNegativeNumberOfBookCopies() throws BookStoreException {
        List<StockBook> booksInStorePreTest = storeManager.getBooks();

        // Try to buy a negative number of copies.
        HashSet<BookCopy> booksToBuy = new HashSet<BookCopy>();
        booksToBuy.add(new BookCopy(TEST_ISBN, -1));

        try {
            client.buyBooks(booksToBuy);
            fail();
        } catch (BookStoreException ex) {
            ;
        }
        List<StockBook> booksInStorePostTest = storeManager.getBooks();
        assertTrue(booksInStorePreTest.containsAll(booksInStorePostTest)
                && booksInStorePreTest.size() == booksInStorePostTest.size());
    }

    /**
     * Tests that all books can be retrieved.
     *
     * @throws BookStoreException the book store exception
     */
    @Test
    public void testGetBooks() throws BookStoreException {
        Set<StockBook> booksAdded = new HashSet<StockBook>();
        booksAdded.add(getDefaultBook());

        Set<StockBook> booksToAdd = new HashSet<StockBook>();
        booksToAdd.add(new ImmutableStockBook(TEST_ISBN + 1, "The Art of Computer Programming", "Donald Knuth",
                (float) 300, NUM_COPIES, 0, 0, 0, false));
        booksToAdd.add(new ImmutableStockBook(TEST_ISBN + 2, "The C Programming Language",
                "Dennis Ritchie and Brian Kerninghan", (float) 50, NUM_COPIES, 0, 0, 0, false));

        booksAdded.addAll(booksToAdd);

        storeManager.addBooks(booksToAdd);

        // Get books in store.
        List<StockBook> listBooks = storeManager.getBooks();

        // Make sure the lists equal each other.
        assertTrue(listBooks.containsAll(booksAdded) && listBooks.size() == booksAdded.size());
    }

    /**
     * Tests that a list of books with a certain feature can be retrieved.
     *
     * @throws BookStoreException the book store exception
     */
    @Test
    public void testGetCertainBooks() throws BookStoreException {
        Set<StockBook> booksToAdd = new HashSet<StockBook>();
        booksToAdd.add(new ImmutableStockBook(TEST_ISBN + 1, "The Art of Computer Programming", "Donald Knuth",
                (float) 300, NUM_COPIES, 0, 0, 0, false));
        booksToAdd.add(new ImmutableStockBook(TEST_ISBN + 2, "The C Programming Language",
                "Dennis Ritchie and Brian Kerninghan", (float) 50, NUM_COPIES, 0, 0, 0, false));

        storeManager.addBooks(booksToAdd);

        // Get a list of ISBNs to retrieved.
        Set<Integer> isbnList = new HashSet<Integer>();
        isbnList.add(TEST_ISBN + 1);
        isbnList.add(TEST_ISBN + 2);

        // Get books with that ISBN.
        List<Book> books = client.getBooks(isbnList);

        // Make sure the lists equal each other
        assertTrue(books.containsAll(booksToAdd) && books.size() == booksToAdd.size());
    }

    /**
     * Tests that books cannot be retrieved if ISBN is invalid.
     *
     * @throws BookStoreException the book store exception
     */
    @Test
    public void testGetInvalidIsbn() throws BookStoreException {
        List<StockBook> booksInStorePreTest = storeManager.getBooks();

        // Make an invalid ISBN.
        HashSet<Integer> isbnList = new HashSet<Integer>();
        isbnList.add(TEST_ISBN); // valid
        isbnList.add(-1); // invalid

        HashSet<BookCopy> booksToBuy = new HashSet<BookCopy>();
        booksToBuy.add(new BookCopy(TEST_ISBN, -1));

        try {
            client.getBooks(isbnList);
            fail();
        } catch (BookStoreException ex) {
            ;
        }

        List<StockBook> booksInStorePostTest = storeManager.getBooks();
        assertTrue(booksInStorePreTest.containsAll(booksInStorePostTest)
                && booksInStorePreTest.size() == booksInStorePostTest.size());
    }

    public class CheckSnapshotRunnable implements Runnable {

        int times;

        public CheckSnapshotRunnable(int times) {
            this.times = times;
        }

        @Override
        public void run() {
            for (int i = 0; i < times; i++) {
                List<StockBook> books = new ArrayList<>();
                try {
                    books = storeManager.getBooks();
                } catch (BookStoreException e) {
                    snapshotError = true;
                    e.printStackTrace();
                }
                for (StockBook book : books) {
                    if (!(book.getNumCopies() == NUM_COPIES || book.getNumCopies() == 0)) {
                        snapshotError = true;
                    }
                }
            }
        }
    }

    public class CheckTopRatedBooksRunnable implements Runnable {

        int times;

        public CheckTopRatedBooksRunnable(int times) {
            this.times = times;
        }

        @Override
        public void run() {
            for (int i = 0; i < times; i++) {
                List<Book> books = new ArrayList<>();
                try {
                    books = storeManager.getBooks().stream().sorted((o1, o2) ->
                            Integer.compare((int) o2.getTotalRating(), (int) o1.getTotalRating())).limit(2).collect(Collectors.toList());
                } catch (BookStoreException e) {
                    totalRatingError = true;
                    e.printStackTrace();
                }
                if (books.get(0).getISBN() != TEST_ISBN + 3 || books.get(1).getISBN() != TEST_ISBN + 2) {
                    totalRatingError = true;
                }
            }
        }
    }

    public class StoreMethodRunnable implements Runnable {
        Object[] storeObject;
        String[] methodName;
        Object[][] args;
        int times;

        public StoreMethodRunnable(Object[] storeObject, String[] methodName, Object[][] args, int times) {
            this.storeObject = storeObject;
            this.methodName = methodName;
            this.args = args;
            this.times = times;
        }

        @Override
        public void run() {
            try {
                List<Method> to_be_invoke_method = new ArrayList<>();
                for (int i = 0; i < methodName.length; i++) {
                    Class<?> storeClass = storeObject[i].getClass();
                    for (Method method : storeClass.getMethods()) {
                        if (!method.getName().equals(methodName[i])) {
                            continue;
                        }
                        Class<?>[] parameterTypes = method.getParameterTypes();
                        boolean matches = true;
                        for (int j = 0; j < parameterTypes.length; j++) {
                            if (!parameterTypes[j].isAssignableFrom(args[i][j].getClass())) {
                                matches = false;
                                break;
                            }
                        }
                        if (matches) {
                            to_be_invoke_method.add(method);
                        } else {
                            throw new NoSuchMethodException();
                        }
                    }
                }
                for (int i = 0; i < times; i++) {
                    for (int j = 0; j < to_be_invoke_method.size(); j++) {
                        to_be_invoke_method.get(j).invoke(storeObject[j], args[j]);
                    }
                }
            } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testReplenish() throws BookStoreException {
        storeManager.removeAllBooks();
        addBooks(TEST_ISBN, NUM_COPIES * 100);
        addBooks(TEST_ISBN + 1, NUM_COPIES * 100);
        addBooks(TEST_ISBN + 2, NUM_COPIES * 100);
        Set<BookCopy> booksToBuy = new HashSet<>();
        booksToBuy.add(new BookCopy(TEST_ISBN, NUM_COPIES));
        booksToBuy.add(new BookCopy(TEST_ISBN + 1, NUM_COPIES));
        booksToBuy.add(new BookCopy(TEST_ISBN + 2, NUM_COPIES));
        Object[][] args = new Object[1][];
        args[0] = new Object[1];
        args[0][0] = booksToBuy;
        Thread c1 = new Thread(new StoreMethodRunnable(new Object[]{client},
                new String[]{"buyBooks"}, args, 100));
        Thread c2 = new Thread(new StoreMethodRunnable(new Object[]{storeManager},
                new String[]{"addCopies"}, args, 100));
        c1.start();
        c2.start();
        try {
            c1.join();
            c2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        HashSet<Integer> checkSet = new HashSet<>(Arrays.asList(TEST_ISBN, TEST_ISBN + 1, TEST_ISBN + 2));
        List<StockBook> result = storeManager.getBooksByISBN(checkSet);
        assertEquals(500, result.get(0).getNumCopies());
        assertEquals(500, result.get(1).getNumCopies());
        assertEquals(500, result.get(2).getNumCopies());
    }

    @Test
    public void testReplenishAndCheck() throws BookStoreException {
        storeManager.removeAllBooks();
        addBooks(TEST_ISBN, NUM_COPIES);
        addBooks(TEST_ISBN + 1, NUM_COPIES);
        addBooks(TEST_ISBN + 2, NUM_COPIES);
        Set<BookCopy> booksToBuyAndReplenish = new HashSet<>();
        booksToBuyAndReplenish.add(new BookCopy(TEST_ISBN, NUM_COPIES));
        booksToBuyAndReplenish.add(new BookCopy(TEST_ISBN + 1, NUM_COPIES));
        booksToBuyAndReplenish.add(new BookCopy(TEST_ISBN + 2, NUM_COPIES));
        Object[][] args = new Object[2][];
        args[0] = new Object[1];
        args[1] = new Object[1];
        args[0][0] = booksToBuyAndReplenish;
        args[1][0] = booksToBuyAndReplenish;
        Thread c1 = new Thread(new StoreMethodRunnable(new Object[]{client, storeManager},
                new String[]{"buyBooks", "addCopies"}, args, 100));
        Thread c2 = new Thread(new CheckSnapshotRunnable(100));
        c1.start();
        c2.start();
        try {
            c1.join();
            c2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertFalse(snapshotError);
    }

    @Test
    public void testPotentialDeadLock() throws BookStoreException {
        storeManager.removeAllBooks();
        for (int i = 1; i < 100000; i++) {
            addBooks(TEST_ISBN + i, NUM_COPIES);
        }
        List<StockBook> initBooks = storeManager.getBooks();
        Set<BookCopy> booksToBuy = storeManager.getBooks().stream()
                .map(stockBook -> new BookCopy(stockBook.getISBN(), 1))
                .collect(Collectors.toSet());
        Object[][] args = new Object[1][];
        args[0] = new Object[1];
        args[0][0] = booksToBuy;
        int TIMES = 1000;
        int NUM_OF_THREADS = 100;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < NUM_OF_THREADS; i++) {
            Thread c = new Thread(new StoreMethodRunnable(new Object[]{storeManager},
                    new String[]{"addCopies"}, args, TIMES));
            threads.add(c);
        }
//        Thread c1 = new Thread(new StoreMethodRunnable(new Object[]{storeManager},
//                new String[]{"addCopies"}, args, TIMES));
//        Thread c2 = new Thread(new StoreMethodRunnable(new Object[]{storeManager},
//                new String[]{"addCopies"}, args, TIMES));
        for (Thread thread : threads) {
            thread.start();
        }
//        c1.start();
//        c2.start();
        try {
            for (Thread thread : threads) thread.join();
            List<StockBook> books = storeManager.getBooks();
            for (int i = 0; i < initBooks.size(); i++) {
                assertEquals(books.get(i).getNumCopies(), initBooks.get(i).getNumCopies() + NUM_OF_THREADS * TIMES);
            }
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void testTotalRating() throws BookStoreException {
        storeManager.removeAllBooks();
        addBooks(TEST_ISBN + 1, NUM_COPIES);
        addBooks(TEST_ISBN + 2, NUM_COPIES);
        addBooks(TEST_ISBN + 3, NUM_COPIES);
        Set<BookRating> bookRatings = storeManager.getBooks().stream()
                .map(stockBook -> new BookRating(stockBook.getISBN(), stockBook.getISBN() - TEST_ISBN)).collect(Collectors.toSet());
        Object[][] args = new Object[1][];
        args[0] = new Object[1];
        args[0][0] = bookRatings;
        Thread c1 = new Thread(new StoreMethodRunnable(new Object[]{client}, new String[]{"rateBooks"}, args, 10000));
        Thread c2 = new Thread(new CheckTopRatedBooksRunnable(10000));
        c1.start();
        c2.start();
        try {
            c1.join();
            c2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertFalse(totalRatingError);
    }

    /**
     * Tear down after class.
     *
     * @throws BookStoreException the book store exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws BookStoreException {
        storeManager.removeAllBooks();

        if (!localTest) {
            ((BookStoreHTTPProxy) client).stop();
            ((StockManagerHTTPProxy) storeManager).stop();
        }
    }
}
