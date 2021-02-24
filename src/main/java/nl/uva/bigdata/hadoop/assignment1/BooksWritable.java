package nl.uva.bigdata.hadoop.assignment1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class BooksWritable  implements Writable {

    private Book[] books;

    public void setBooks(Book[] books) {
        this.books = books;
    }

    /*
     * write
     * Write function that writes the Book array to a DataOutput buffer.
     * Data structure:
     *      <int>number_of_books
     *      <String>title_book_1 <int>year_book_1
     *      <String>title_book_2 <int>year_book_2
     *      etc.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        // Write number of books to buffer
        out.writeInt(books.length);

        // For each book write title as UTF and year as int
        for (int i=0; i < books.length; i++) {
            out.writeUTF(books[i].getTitle());
            out.writeInt(books[i].getYear());
        }

    }

    /*
     * readFields
     * Read function that reads an InputOutput buffer and stores it as a Book array.
     * Expected data structure:
     *      <int>number_of_books
     *      <String>title_book_1 <int>year_book_1
     *      <String>title_book_2 <int>year_book_2
     *      etc.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // Read number of books from buffer
        int number = in.readInt();
        // Create a new Book array
        Book[] new_books = new Book[number];

        // For each book read title as UTF and year as int
        for (int i = 0; i < number; i++) {
            String title = in.readUTF();
            int year = in.readInt();
            new_books[i] = new Book(title, year);
        }
        // Update this.books using setBooks()
        this.setBooks(new_books);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BooksWritable that = (BooksWritable) o;
        return Arrays.equals(books, that.books);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(books);
    }
}
