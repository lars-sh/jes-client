package de.larssh.jes;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * This enumeration contains all JES job status. In addition {@link #ALL} works
 * as a wildcard value.
 */
@Getter
@RequiredArgsConstructor
public enum JobStatus {
	/**
	 * Wildcard value for any of the following values.
	 */
	ALL("ALL"),

	/**
	 * Status of input (requested) jobs, that were not started, yet.
	 */
	INPUT("INPUT"),

	/**
	 * Status of currently active (running) jobs.
	 */
	ACTIVE("ACTIVE"),

	/**
	 * Status of held (finished) jobs.
	 */
	OUTPUT("OUTPUT");

	/**
	 * Value to be used for JES communication
	 *
	 * @return status value
	 */
	String value;
}
