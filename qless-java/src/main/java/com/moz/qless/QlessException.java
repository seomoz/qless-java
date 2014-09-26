package com.moz.qless;

/**
 * Our customized QlessException
 *
 */
@SuppressWarnings("serial")
public class QlessException extends RuntimeException {
  public QlessException() {
  }

  public QlessException(final String message) {
    super(message);
  }

  public QlessException(final Throwable cause) {
    super(cause);
  }

  public QlessException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
