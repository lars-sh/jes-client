package de.larssh.jes;

import java.util.regex.Pattern;

import lombok.Getter;

/**
 * This enumeration contains job flags, such as special status a job can be in.
 */
@Getter
public enum JobFlag {
	/**
	 * Flag for dup. jobs.
	 */
	DUP("-DUP-"),

	/**
	 * Flag for jobs with JCL error.
	 */
	JCL_ERROR("\\(JCL error\\)"),

	/**
	 * Flag for held jobs.
	 */
	HELD("-HELD-");

	/**
	 * Pattern used for JES communication to parse jobs rest values
	 *
	 * @return pattern used for JES communication to parse jobs rest values
	 */
	Pattern restPattern;

	/**
	 * This enumeration contains job flags, such as special status a job can be in.
	 *
	 * @param restPattern pattern used for JES communication to parse jobs rest
	 *                    values
	 */
	JobFlag(final String restPattern) {
		this.restPattern = Pattern.compile(restPattern);
	}
}
