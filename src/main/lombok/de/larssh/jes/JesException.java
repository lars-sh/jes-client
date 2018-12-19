package de.larssh.jes;

import org.apache.commons.net.ftp.FTPClient;

import de.larssh.utils.text.Strings;
import lombok.ToString;

/**
 * Thrown to indicate that a logical JES exception occurred.
 */
@ToString
public class JesException extends Exception {
	// @EqualsAndHashCode(callSuper = true, onParam_ = { @Nullable })

	private static final long serialVersionUID = 4049707552379185213L;

	/**
	 * Constructs a new {@link JesException} with the given message, formatting as
	 * described at {@link Strings#format(String, Object...)}.
	 *
	 * @param message   the detail message
	 * @param arguments arguments referenced by format specifiers in {@code message}
	 * @throws java.util.IllegalFormatException {@code arguments} is not empty and
	 *         {@code format} contains unexpected syntax
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
	 * @throws java.util.IllegalFormatException {@code arguments} is not empty and
	 *         {@code format} contains unexpected syntax
	 */
	public JesException(final FTPClient ftpClient, final String message, final Object... arguments) {
		this("%s Reason: %s", Strings.format(message, arguments), ftpClient.getReplyString());
	}
}
