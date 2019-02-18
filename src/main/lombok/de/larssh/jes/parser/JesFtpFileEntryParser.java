package de.larssh.jes.parser;

import static de.larssh.utils.Finals.constant;
import static de.larssh.utils.text.Strings.NEW_LINE;
import static java.util.stream.Collectors.joining;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileEntryParser;

import de.larssh.jes.Job;
import de.larssh.jes.JobOutput;
import de.larssh.jes.JobStatus;
import de.larssh.utils.Optionals;
import de.larssh.utils.text.Patterns;
import de.larssh.utils.text.Strings;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.NoArgsConstructor;

/**
 * Implementation of {@link FTPFileEntryParser} for IBM z/OS JES spools,
 * converting that information into a {@link JesFtpFile} instance.
 */
@NoArgsConstructor
public class JesFtpFileEntryParser implements FTPFileEntryParser {
	/**
	 * Minimum number of lines of a job details entry
	 */
	private static final int JOB_OUTPUT_DETAILS_LINES_MIN = constant(4);

	/**
	 * Pattern to match the job details header response line
	 */
	private static final Pattern PATTERN_TITLE = Pattern.compile("^JOBNAME +JOBID +OWNER +STATUS +CLASS *$");

	/**
	 * Pattern to match job details lines inside response
	 *
	 * <p>
	 * List of named groups:
	 * <ul>
	 * <li>name
	 * <li>id
	 * <li>owner
	 * <li>status
	 * <li>class
	 * <li>rest
	 * </ul>
	 */
	private static final Pattern PATTERN_JOB = Pattern.compile(
			"^(?<name>.{8}) (?<id>.{8}) (?<owner>.{8}) (?<status>(INPUT|ACTIVE|OUTPUT)) (?<class>.{1,8})( (?<rest>.*))?$");

	/**
	 * Pattern to find the result code inside a response line
	 *
	 * <p>
	 * List of named groups:
	 * <ul>
	 * <li>returnCode
	 * </ul>
	 */
	private static final Pattern PATTERN_JOB_RETURN_CODE = Pattern.compile("RC=(?<returnCode>\\d+)");

	/**
	 * Pattern to find the abend code inside a response line
	 *
	 * <p>
	 * List of named groups:
	 * <ul>
	 * <li>abend
	 * </ul>
	 */
	private static final Pattern PATTERN_JOB_ABEND = Pattern.compile("ABEND=(?<abend>\\S+)");

	/**
	 * Pattern to match the separator response line
	 */
	private static final Pattern PATTERN_SEPARATOR = Pattern.compile("^-+ *$");

	/**
	 * Pattern to match the job output header response line
	 */
	private static final String PATTERN_SUB_TITLE = constant("         ID  STEPNAME PROCSTEP C DDNAME   BYTE-COUNT  ");

	/**
	 * Pattern to match job output lines inside response
	 *
	 * <p>
	 * List of named groups:
	 * <ul>
	 * <li>index
	 * <li>step
	 * <li>procedureStep
	 * <li>class
	 * <li>name
	 * <li>length
	 * </ul>
	 */
	private static final Pattern PATTERN_JOB_OUTPUT = Pattern.compile(
			"^ {9}(?<index>\\d{3}) (?<step>.{8}) (?<procedureStep>.{8}) (?<class>.) (?<name>.{8}) {1,9}(?<length>\\d{1,9}) $");

	/**
	 * Pattern to match the response line containing the number of spool files
	 */
	private static final Pattern PATTERN_SPOOL_FILES = Pattern.compile("^\\d+ spool files *$");

	/**
	 * Builds a {@link Job} object based on a job details response {@code line}.
	 *
	 * @param line job details response line
	 * @return {@link Job} object
	 */
	private Job createJob(final String line) {
		final Matcher matcher = Patterns.matches(PATTERN_JOB, line)
				.orElseThrow(() -> new JesFtpFileEntryParserException("Expected [%s] as job details line, got [%s].",
						PATTERN_JOB.pattern(),
						line));

		// Checkstyle: Ignore duplicate "name" for 2 lines
		// Checkstyle: Ignore duplicate "class" for 5 lines
		final String id = matcher.group("id");
		final String name = matcher.group("name");
		final JobStatus status = JobStatus.valueOf(matcher.group("status"));
		final String owner = matcher.group("owner");
		final Optional<String> jesClass = Optionals.ofNonBlank(matcher.group("class"));

		final Optional<String> rest = Optionals.ofNonBlank(matcher.group("rest"));
		final OptionalInt resultCode = Optionals.mapToInt(
				rest.flatMap(r -> Patterns.find(PATTERN_JOB_RETURN_CODE, r)).map(m -> m.group("returnCode")),
				Integer::parseInt);
		final Optional<String> abendCode
				= rest.flatMap(r -> Patterns.find(PATTERN_JOB_ABEND, r)).map(m -> m.group("abend"));

		return new Job(id, name, status, owner, jesClass, resultCode, abendCode);
	}

	/**
	 * Parses {@code listEntry} and builds a {@link Job} object based on the given
	 * content.
	 *
	 * <p>
	 * Job details and job outputs were joined together using
	 * {@link #preParse(List)} before.
	 *
	 * @param listEntry one logical line from the file listing
	 * @return {@link Job} object
	 * @throws JesFtpFileEntryParserException on unexpected {@code listEntry}
	 */
	private Job createJobAndOutputs(final String listEntry) {
		final List<String> lines = new ArrayList<>(Strings.getLines(listEntry));
		if (lines.isEmpty()) {
			lines.add("");
		}

		// First line (job)
		final Job job = createJob(lines.remove(0));
		if (lines.isEmpty()) {
			return job;
		}
		if (lines.size() < JOB_OUTPUT_DETAILS_LINES_MIN) {
			throw new JesFtpFileEntryParserException("Expected %d lines, got %d. List entry: %s",
					JOB_OUTPUT_DETAILS_LINES_MIN,
					lines.size(),
					listEntry);
		}

		// Second line (separator)
		final String separator = lines.remove(0);
		if (!separator.equals(PATTERN_SEPARATOR)) {
			throw new JesFtpFileEntryParserException("Expected [%s] as separator line, got [%s].",
					PATTERN_SEPARATOR,
					separator);
		}

		// Third line (sub title)
		final String subTitle = lines.remove(0);
		if (!subTitle.equals(PATTERN_SUB_TITLE)) {
			throw new JesFtpFileEntryParserException("Expected [%s] as sub title line, got [%s].",
					PATTERN_SUB_TITLE,
					subTitle);
		}

		// Last line (spool files)
		final String spoolFiles = lines.remove(lines.size() - 1);
		if (!Patterns.matches(PATTERN_SPOOL_FILES, spoolFiles).isPresent()) {
			throw new JesFtpFileEntryParserException("Expected [%s] as spool files line, got [%s].",
					PATTERN_SPOOL_FILES.pattern(),
					spoolFiles);
		}

		// Further lines (job outputs)
		for (final String line : lines) {
			createJobOutput(job, line);
		}
		return job;
	}

	/**
	 * Builds a {@link JobOutput} object based on a job output response
	 * {@code line}.
	 *
	 * @param job  related job
	 * @param line job details response line
	 * @return {@link JobOutput} object
	 */
	private JobOutput createJobOutput(final Job job, final String line) {
		final Matcher matcher
				= Patterns.matches(PATTERN_JOB_OUTPUT, line)
						.orElseThrow(() -> new JesFtpFileEntryParserException(
								"Expected [%s] as job output details line, got [%s].",
								PATTERN_JOB_OUTPUT.pattern(),
								line));

		// Checkstyle: Ignore duplicate "name" for 6 lines
		// Checkstyle: Ignore duplicate "class" for 6 lines
		final int index = Integer.parseInt(matcher.group("index"));
		final String name = matcher.group("name");
		final int length = Integer.parseInt(matcher.group("length"));
		final Optional<String> step = Optionals.ofNonBlank(matcher.group("step"));
		final Optional<String> procedureStep = Optionals.ofNonBlank(matcher.group("procedureStep"));
		final Optional<String> jesClass = Optionals.ofNonBlank(matcher.group("class"));

		return job.createOutput(index, name, length, step, procedureStep, jesClass);
	}

	/** {@inheritDoc} */
	@Nullable
	@Override
	public FTPFile parseFTPEntry(@Nullable final String listEntry) {
		Objects.requireNonNull(listEntry);
		return new JesFtpFile(createJobAndOutputs(listEntry), listEntry);
	}

	/** {@inheritDoc} */
	@Nullable
	@Override
	public String readNextEntry(@Nullable final BufferedReader reader) throws IOException {
		Objects.requireNonNull(reader);
		return reader.readLine();
	}

	/** {@inheritDoc} */
	@NonNull
	@Override
	@SuppressFBWarnings(value = "CFS_CONFUSING_FUNCTION_SEMANTICS", justification = "based on interface contract")
	public List<String> preParse(@Nullable final List<String> original) {
		Objects.requireNonNull(original);

		// Empty list
		if (original.isEmpty()) {
			throw new JesFtpFileEntryParserException("Parsing JES job details failed. No line found.");
		}

		// First line
		if (!Patterns.matches(PATTERN_TITLE, original.get(0)).isPresent()) {
			throw new JesFtpFileEntryParserException("Parsing JES job details failed. Unexpected first line: [%s].",
					original.get(0));
		}

		// Iterate over original from 1 to size
		// 1. ignore title line (starting at 1)
		// 2. concatenate lines of the same job to one list entry
		// 3. handle last line (stopping at size)
		final List<String> lines = new ArrayList<>();
		final List<String> linesOfCurrentJob = new ArrayList<>();
		final int size = original.size();
		for (int index = 1; index <= size; index += 1) {
			final boolean isLast = index >= size;
			final Optional<String> line = isLast ? Optional.empty() : Optional.of(original.get(index));

			if ((line.flatMap(l -> Patterns.matches(PATTERN_JOB, l)).isPresent() || isLast)
					&& !linesOfCurrentJob.isEmpty()) {
				lines.add(linesOfCurrentJob.stream().collect(joining(NEW_LINE)));
				linesOfCurrentJob.clear();
			}
			line.ifPresent(linesOfCurrentJob::add);
		}

		// The interface requires us to return "original" - nothing else!
		original.clear();
		original.addAll(lines);
		return original;
	}
}
