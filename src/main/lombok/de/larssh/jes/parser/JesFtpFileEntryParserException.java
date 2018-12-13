package de.larssh.jes.parser;

import de.larssh.utils.text.Strings;
import lombok.ToString;

/**
 * Thrown to indicate that parsing the JES job list failed.
 */
@ToString
// @EqualsAndHashCode(callSuper = true, onParam_ = { @Nullable })
public class JesFtpFileEntryParserException extends RuntimeException {
	private static final long serialVersionUID = 5139270814035992446L;

	/**
	 * Constructs a new {@link JesFtpFileEntryParserException} with the given
	 * message, formatting as described at
	 * {@link Strings#format(String, Object...)}.
	 *
	 * @param message   the detail message
	 * @param arguments arguments referenced by format specifiers in {@code message}
	 * @throws java.util.IllegalFormatException {@code arguments} is not empty and
	 *         {@code format} contains unexpected syntax
	 */
	public JesFtpFileEntryParserException(final String message, final Object... arguments) {
		super(Strings.format(message, arguments));
	}
}
