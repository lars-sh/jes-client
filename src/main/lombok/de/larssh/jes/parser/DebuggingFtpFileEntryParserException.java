package de.larssh.jes.parser;

import org.apache.commons.net.ftp.FTPFileEntryParser;

import de.larssh.utils.text.Strings;
import lombok.Getter;

/**
 * Thrown to indicate that a {@link FTPFileEntryParser} function is called.
 */
@Getter
public class DebuggingFtpFileEntryParserException extends RuntimeException {
	// @EqualsAndHashCode(callSuper = true, onParam_ = { @Nullable })

	private static final long serialVersionUID = -3777777047932012565L;

	String value;

	/**
	 * Constructs a new {@link DebuggingFtpFileEntryParserException} for the given
	 * method name and its stringified value.
	 *
	 * @param methodName the called methods name
	 * @param value      the stringified value supplied to the called method
	 */
	public DebuggingFtpFileEntryParserException(final String methodName, final String value) {
		super(Strings.format("Method %s called using:%s%s", methodName, Strings.NEW_LINE, value));
		initCause(null);

		this.value = value;
	}
}
