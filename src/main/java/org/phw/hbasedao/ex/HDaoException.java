package org.phw.hbasedao.ex;

public class HDaoException extends Exception {
    private static final long serialVersionUID = -7165654285787849791L;

    /**
     * Constructs a new exception with <code>null</code> as its detail message.
     * The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     */
    public HDaoException() {
        super();
    }

    /**
     * Constructs a new HDaoException with the specified detail message.  The
     * cause is not initialized, and may subsequently be initialized by
     * a call to {@link #initCause}.
     *
     * @param   message   the detail message. The detail message is saved for 
     *          later retrieval by the {@link #getMessage()} method.
     */
    public HDaoException(String message) {
        super(message);
    }

    /**
     * Constructs a new HDaoException with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * <code>cause</code> is <i>not</i> automatically incorporated in
     * this HDaoException's detail message.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     * @since  1.4
     */
    public HDaoException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new HDaoException with the specified cause and a detail
     * message of <tt>(cause==null ? null : cause.toString())</tt> (which
     * typically contains the class and detail message of <tt>cause</tt>).
     * This constructor is useful for HDaoExceptions that are little more than
     * wrappers for other throwables (for example, {@link
     * java.security.PrivilegedActionHDaoException}).
     *
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     * @since  1.4
     */
    public HDaoException(Throwable cause) {
        super(cause);
    }
}
