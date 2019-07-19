package de.larssh.jes;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import de.larssh.utils.OptionalInts;
import de.larssh.utils.text.Strings;
import edu.umd.cs.findbugs.annotations.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Value object containing a jobs status information. Depending on the objects
 * creating code not all fields are present.
 *
 * <p>
 * Two {@code Job} objects are equal if their IDs are equal. Therefore two
 * objects with the same ID, but different job status (e.g. one older and one
 * up-to-date object) are still equal!
 */
@Getter
@ToString
@SuppressWarnings("PMD.ShortClassName")
@EqualsAndHashCode(onlyExplicitlyIncluded = true, onParam_ = { @Nullable })
public class Job {
	/**
	 * The jobs ID, must not be empty
	 *
	 * @return job id
	 */
	@EqualsAndHashCode.Include
	@SuppressWarnings("PMD.ShortVariable")
	String id;

	/**
	 * The jobs name
	 *
	 * <p>
	 * Depending on the objects creating code this might be a filter value,
	 * eventually containing {@link JesClient#FILTER_WILDCARD}.
	 *
	 * @return job name
	 */
	String name;

	/**
	 * The jobs status
	 *
	 * @return job status
	 */
	JobStatus status;

	/**
	 * The jobs owner
	 *
	 * <p>
	 * Depending on the objects creating code this might be a filter value,
	 * eventually containing {@link JesClient#FILTER_WILDCARD}.
	 *
	 * @return job owner
	 */
	String owner;

	/**
	 * The jobs JES spool class
	 *
	 * @return job class
	 */
	Optional<String> jesClass;

	/**
	 * The jobs result code
	 *
	 * <p>
	 * This field can be present for {@link JobStatus#OUTPUT} (and
	 * {@link JobStatus#ALL}) only. It cannot be present as long as
	 * {@code abendCode} is present.
	 *
	 * @return result code
	 */
	OptionalInt resultCode;

	/**
	 * The jobs abend code
	 *
	 * <p>
	 * This field can be present for {@link JobStatus#OUTPUT} (and
	 * {@link JobStatus#ALL}) only. It cannot be present as long as
	 * {@code resultCode} is present.
	 *
	 * @return abend code
	 */
	Optional<String> abendCode;

	/**
	 * The jobs flags
	 *
	 * @return flags
	 */
	Set<JobFlag> flags;

	/**
	 * List of job output details
	 *
	 * <p>
	 * Can be filled by
	 * {@link #createOutput(int, String, int, Optional, Optional, Optional)} and
	 * must be empty for status other than {@link JobStatus#OUTPUT} (and
	 * {@link JobStatus#ALL}).
	 *
	 * @return list of job outputs
	 */
	List<JobOutput> outputs = new ArrayList<>();

	/**
	 * This constructor creates a {@link Job} in its simplest form. String
	 * parameters are trimmed and converted to upper case.
	 *
	 * @param jobId  the jobs ID, must not be empty
	 * @param name   the jobs name or a name filter value
	 * @param status the jobs status or {@link JobStatus#ALL}
	 * @param owner  the jobs owner or an owner filter value
	 * @throws JobFieldInconsistentException on inconsistent field value
	 */
	public Job(final String jobId, final String name, final JobStatus status, final String owner) {
		this(jobId, name, status, owner, Optional.empty(), OptionalInt.empty(), Optional.empty());
	}

	/**
	 * This constructor creates a {@link Job} allowing to set any field. String
	 * parameters are trimmed and converted to upper case.
	 *
	 * @param jobId      the jobs ID, must not be empty
	 * @param name       the jobs name or a name filter value
	 * @param status     the jobs status or {@link JobStatus#ALL}
	 * @param owner      the jobs owner or an owner filter value
	 * @param jesClass   the jobs JES class
	 * @param resultCode the jobs result code, when held (finished)
	 * @param abendCode  the jobs abend code, when held (finished)
	 * @param flags      the jobs flags
	 * @throws JobFieldInconsistentException on inconsistent field value
	 */
	@SuppressWarnings("checkstyle:ParameterNumber")
	public Job(final String jobId,
			final String name,
			final JobStatus status,
			final String owner,
			final Optional<String> jesClass,
			final OptionalInt resultCode,
			final Optional<String> abendCode,
			final JobFlag... flags) {
		id = Strings.toNeutralUpperCase(jobId.trim());
		this.name = Strings.toNeutralUpperCase(name.trim());
		this.status = status;
		this.owner = Strings.toNeutralUpperCase(owner.trim());
		this.jesClass = jesClass.map(String::trim).map(Strings::toNeutralUpperCase);
		this.abendCode = abendCode.map(String::trim).map(Strings::toNeutralUpperCase);
		this.resultCode = resultCode;
		this.flags = unmodifiableSet(new HashSet<>(asList(flags)));

		validate();
	}

	/**
	 * Creates a {@link JobOutput} and adds it to the list of job outputs. Creating
	 * of job outputs for status other than {@link JobStatus#OUTPUT} (and
	 * {@link JobStatus#ALL}) is prohibited. String parameters are trimmed and
	 * converted to upper case.
	 *
	 * @param index         the job outputs index inside the jobs list of outputs
	 *                      (starting at 1)
	 * @param outputName    the job outputs data division name
	 * @param length        the job outputs content length
	 * @param step          the job outputs step name
	 * @param procedureStep the job outputs procedure step name
	 * @param outputClass   the output class
	 * @return created job output
	 * @throws JobFieldInconsistentException on inconsistent field value
	 */
	public JobOutput createOutput(final int index,
			final String outputName,
			final int length,
			final Optional<String> step,
			final Optional<String> procedureStep,
			final Optional<String> outputClass) {
		if (getStatus() != JobStatus.OUTPUT && getStatus() != JobStatus.ALL) {
			throw new JobFieldInconsistentException(
					"Job outputs for status other than OUTPUT (and ALL) cannot be created. Status: %s",
					getStatus());
		}

		final JobOutput output = new JobOutput(this, index, outputName, length, step, procedureStep, outputClass);
		outputs.add(output);
		return output;
	}

	/**
	 * Returns the job output details referenced by {@code name} or
	 * {@link Optional#empty()} if no such job output exists.
	 *
	 * @param name job output name to search for
	 * @return job output details referenced by {@code name} or
	 *         {@link Optional#empty()} if no such job output exists
	 */
	public Optional<JobOutput> getOutput(final String name) {
		return getOutputs().stream().filter(output -> output.getName().equals(name)).findFirst();
	}

	/**
	 * List of job output details
	 *
	 * <p>
	 * Can be filled by
	 * {@link #createOutput(int, String, int, Optional, Optional, Optional)} and
	 * must be empty for status other than {@link JobStatus#OUTPUT} (and
	 * {@link JobStatus#ALL}).
	 *
	 * @return list of job output details
	 */
	public List<JobOutput> getOutputs() {
		return unmodifiableList(outputs);
	}

	/**
	 * Validates fields to be set in a consistent way.
	 *
	 * @throws JobFieldInconsistentException on inconsistent field value
	 */
	@SuppressWarnings({ "PMD.CyclomaticComplexity", "PMD.NPathComplexity" })
	private void validate() {
		if (id.isEmpty()) {
			throw new JobFieldInconsistentException("Job ID must be filled, but is empty.");
		}
		if (jesClass.filter(String::isEmpty).isPresent()) {
			throw new JobFieldInconsistentException("JES class must not be empty if present.");
		}
		if (resultCode.isPresent() && abendCode.isPresent()) {
			throw new JobFieldInconsistentException("Result Code and Abend Code must not be present at the same time.");
		}
		if (OptionalInts.filter(resultCode, r -> r < 0).isPresent()) {
			throw new JobFieldInconsistentException("Result Code must not be less than zero.");
		}
		if (abendCode.filter(String::isEmpty).isPresent()) {
			throw new JobFieldInconsistentException("Abend Code must not be empty if present.");
		}
		if (status != JobStatus.OUTPUT && status != JobStatus.ALL) {
			if (resultCode.isPresent()) {
				throw new JobFieldInconsistentException(
						Strings.format("Result Code must not be present for status [%s].", status));
			}
			if (abendCode.isPresent()) {
				throw new JobFieldInconsistentException(
						Strings.format("Abend Code must not be present for status [%s].", status));
			}
		}
	}
}
