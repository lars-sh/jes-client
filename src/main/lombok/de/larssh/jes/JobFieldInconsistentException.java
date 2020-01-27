package de.larssh.jes;

import de.larssh.utils.text.Strings;

/**
 * Thrown to indicate that fields of a {@link Job} or {@link JobOutput} object
 * are inconsistent.
 */
public class JobFieldInconsistentException extends RuntimeException {
	private static final long serialVersionUID = -1395354329013583080L;

	/**
	 * Constructs a new {@link JobFieldInconsistentException} with the given
	 * message, formatting as described at
	 * {@link Strings#format(String, Object...)}.
	 *
	 * @param message   the detail message
	 * @param arguments arguments referenced by format specifiers in {@code message}
	 */
	public JobFieldInconsistentException(final String message, final Object... arguments) {
		super(Strings.format(message, arguments), null);
	}
}
