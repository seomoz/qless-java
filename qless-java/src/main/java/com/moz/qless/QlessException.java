package com.moz.qless;

public class QlessException extends RuntimeException {
  private static final long serialVersionUID = -772350861057729943L;

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
