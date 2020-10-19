package de.larssh.jes.parser;

import de.larssh.utils.text.Strings;

/**
 * Thrown to indicate that parsing the JES job list failed.
 */
public class JesFtpFileEntryParserException extends RuntimeException {
	/**
	 * Constructs a new {@link JesFtpFileEntryParserException} with the given
	 * message, formatting as described at
	 * {@link Strings#format(String, Object...)}.
	 *
	 * @param message   the detail message
	 * @param arguments arguments referenced by format specifiers in {@code message}
	 */
	public JesFtpFileEntryParserException(final String message, final Object... arguments) {
		super(Strings.format(message, arguments), null);
	}
}
