package de.larssh.jes.parser;

import de.larssh.utils.annotations.SuppressJacocoGenerated;
import de.larssh.utils.text.Strings;
import lombok.Getter;

/**
 * Thrown to indicate that a
 * {@link org.apache.commons.net.ftp.FTPFileEntryParser} function is called.
 */
@Getter
@SuppressJacocoGenerated(justification = "non-productive class, meant to be used for debugging purposes only")
public class DebuggingFtpFileEntryParserException extends RuntimeException {
	// @EqualsAndHashCode(callSuper = true, onParam_ = { @Nullable })

	private static final long serialVersionUID = -3777777047932012565L;

	/**
	 * The called methods name
	 *
	 * @return the called methods name
	 */
	String methodName;

	/**
	 * The stringified value supplied to the called method
	 *
	 * @return value the stringified value supplied to the called method
	 */
	String value;

	/**
	 * Constructs a new {@link DebuggingFtpFileEntryParserException} for the given
	 * method name and its stringified value.
	 *
	 * @param methodName the called methods name
	 * @param value      the stringified value supplied to the called method
	 */
	public DebuggingFtpFileEntryParserException(final String methodName, final String value) {
		super(Strings.format("Method %s called using:%s%s", methodName, Strings.NEW_LINE, value), null);

		this.methodName = methodName;
		this.value = value;
	}
}
