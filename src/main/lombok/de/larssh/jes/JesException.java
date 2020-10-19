package de.larssh.jes;

import org.apache.commons.net.ftp.FTPClient;

import de.larssh.utils.text.Strings;

/**
 * Thrown to indicate that a logical JES exception occurred.
 */
public class JesException extends Exception {
	/**
	 * Constructs a new {@link JesException} with the given message, formatting as
	 * described at {@link Strings#format(String, Object...)}.
	 *
	 * @param message   the detail message
	 * @param arguments arguments referenced by format specifiers in {@code message}
	 */
	public JesException(final String message, final Object... arguments) {
		super(Strings.format(message, arguments), null);
	}

	/**
	 * Constructs a new {@link JesException} with the given message, formatting as
	 * described at {@link Strings#format(String, Object...)}, appending the latest
	 * FTP reply string.
	 *
	 * @param ftpClient FTP client with reply string
	 * @param message   the detail message
	 * @param arguments arguments referenced by format specifiers in {@code message}
	 */
	public JesException(final FTPClient ftpClient, final String message, final Object... arguments) {
		this("%s Reason: %s", Strings.format(message, arguments), ftpClient.getReplyString());
	}
}
