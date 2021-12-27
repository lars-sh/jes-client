package de.larssh.jes;

import static de.larssh.utils.Collectors.toLinkedHashMap;
import static de.larssh.utils.Finals.constant;
import static de.larssh.utils.function.ThrowingFunction.throwing;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import de.larssh.jes.parser.JesFtpFile;
import de.larssh.jes.parser.JesFtpFileEntryParserFactory;
import de.larssh.utils.Nullables;
import de.larssh.utils.Optionals;
import de.larssh.utils.annotations.SuppressJacocoGenerated;
import de.larssh.utils.function.ThrowingConsumer;
import de.larssh.utils.text.Patterns;
import de.larssh.utils.text.Strings;
import de.larssh.utils.time.Stopwatch;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import lombok.experimental.NonFinal;

/**
 * This class allows to handle IBM z/OS JES spools using Java technologies. The
 * used interface is the IBM z/OS FTP server, that should be available by
 * default.
 *
 * <p>
 * JES spool entries can be filtered and listed using
 * {@link #list(String, JobStatus, String)} and
 * {@link #listFilled(String, JobStatus, String)} methods, while the later one
 * gathers more information, but takes some more time.
 *
 * <p>
 * {@link #submit(String)} submits JCLs based on the FTP users permissions.
 * {@link #waitFor(Job, Duration, Duration)} can be used to wait until a job
 * terminates. Job outputs can be retrieved using {@link #retrieve(JobOutput)}
 * and removed using {@link #delete(Job)}.
 *
 * <p>
 * <b>Usage example:</b> The following shows the JesClient used inside a
 * try-with-resource statement. The constructor descriptions describe further
 * details.
 *
 * <pre>
 * // Connect and login via simplified constructor
 * try (JesClient jesClient = new JesClient(hostname, port, username, password)) {
 *
 *     // Submit JCL
 *     Job job = jesClient.submit(jclContent);
 *
 *     // Wait for job to be finished
 *     if (!jesClient.waitFor(job)) {
 *         // Handle the case, a finished job cannot be found inside JES spool any longer
 *         throw ...;
 *     }
 *
 *     // Gather job status details
 *     Job detailedJob = jesClient.getJobDetails(job);
 *
 *     // Gather finished jobs outputs
 *     List&lt;JobOutput&gt; jobOutput = jesClient.get(job);
 *
 *     // Delete job from JES spool
 *     jesClient.delete(job);
 *
 * // Logout and disconnect using try-with-resource (close method)
 * }
 * </pre>
 *
 * <p>
 * In case filtering jobs does not work as expected, check the JES Interface
 * Level of your server using {@link #getServerProperties()}. This class
 * requires {@code JESINTERFACELEVEL = 2}. The JES Interface Level can be
 * configured by a mainframe administrator inside {@code FTP.DATA}.
 *
 * @see <a href=
 *      "https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.3.0/com.ibm.zos.v2r3.halu001/intfjes.htm">IBM
 *      Knowledge Center - Interfacing with JES</a>
 */
@Getter
@SuppressWarnings({ "PMD.ExcessiveImports", "PMD.GodClass" })
public class JesClient implements Closeable {
	/**
	 * Wildcard value to be used for name and owner filters, meaning "any" value.
	 */
	public static final String FILTER_WILDCARD = constant("*");

	/**
	 * Charset, that is used for submitting JCLs and retrieving job outputs.
	 */
	private static final Charset FTP_DATA_CHARSET = StandardCharsets.UTF_8;

	/**
	 * Maximum limit of spool entries (including)
	 */
	public static final int LIST_LIMIT_MAX = constant(1024);

	/**
	 * Limit of spool entries for {@link #exists(Job, JobStatus)}
	 *
	 * <p>
	 * Checking for existence does not need a limit, but using a limit allows to
	 * handle an additional error case.
	 */
	private static final int LIST_LIMIT_EXISTS = 2;

	/**
	 * Minimum limit of spool entries (including)
	 */
	private static final int LIST_LIMIT_MIN = 1;

	/**
	 * Pattern to find the job ID inside the FTP response after submitting a JCL.
	 */
	private static final Pattern PATTERN_FTP_SUBMIT_ID = Pattern.compile("^250-IT IS KNOWN TO JES AS (?<id>\\S+)");

	/**
	 * Pattern to find the job name inside a valid JCL.
	 */
	private static final Pattern PATTERN_JCL_JOB_NAME = Pattern.compile("^//\\s*(?<name>\\S+)");

	/**
	 * Pattern to check the response string for the spool entries limit warning.
	 */
	private static final Pattern PATTERN_LIST_LIMIT
			= Pattern.compile("^250-JESENTRYLIMIT OF \\d+ REACHED\\.  ADDITIONAL ENTRIES NOT DISPLAYED$");

	/**
	 * Pattern to check the response string for the empty list warning.
	 */
	private static final Pattern PATTERN_LIST_NAMES_NO_JOBS_FOUND = Pattern.compile("^550 NO JOBS FOUND FOR ");

	/**
	 * Pattern to retrieve status values from response strings.
	 */
	private static final Pattern PATTERN_STATUS = Pattern
			.compile("^211-(SERVER SITE VARIABLE |TIMER )?(?<key>\\S+)( VALUE)? IS (SET TO )?(?<value>\\S+?)\\.?$");

	/**
	 * Remote file name that is used when submitting a JCL.
	 */
	private static final String SUBMIT_REMOTE_FILE_NAME = JesClient.class.getSimpleName() + ".jcl";

	/**
	 * FTP Client used by the current JES client instance.
	 *
	 * @return FTP client
	 */
	FTPClient ftpClient;

	/**
	 * Current JES spool user
	 *
	 * @return JES spool owner
	 */
	@NonFinal
	String jesOwner = FILTER_WILDCARD;

	/**
	 * Expert constructor. This constructor creates a FTP client <b>without</b>
	 * connecting and logging in. It is meant to be used in scenarios, which require
	 * additional FTP configuration.
	 *
	 * <p>
	 * <b>Usage example 1</b> (using a simplified login)
	 *
	 * <pre>
	 * // Construct the JES client and its internal FTP client
	 * try (JesClient jesClient = new JesClient()) {
	 *
	 *     // Connect via FTP
	 *     jesClient.getFtpClient().connect(...);
	 *
	 *     // Simplified login using the JES client
	 *     jesClient.login(...);
	 *
	 *     ...
	 *
	 * // Logout and disconnect using try-with-resource (close method)
	 * }
	 * </pre>
	 *
	 * <p>
	 * <b>Usage example 2:</b> (using a custom login)
	 *
	 * <pre>
	 * // Construct the JES client and its internal FTP client
	 * try (JesClient jesClient = new JesClient()) {
	 *
	 *     // Connect via FTP
	 *     jesClient.getFtpClient().connect(...);
	 *
	 *     // Login via FTP client
	 *     jesClient.getFtpClient().login(...);
	 *
	 *     // Set the JES spool owner
	 *     jesClient.setJesOwner(...);
	 *
	 *     // Enter JES mode of the FTP connection
	 *     jesClient.enterJesMode();
	 *
	 *     ...
	 *
	 * // Logout and disconnect using try-with-resource (close method)
	 * }
	 * </pre>
	 */
	public JesClient() {
		ftpClient = new FTPClient();
		ftpClient.setParserFactory(new JesFtpFileEntryParserFactory());
	}

	/**
	 * Simplified constructor. This constructor initiates a new FTP connection and
	 * logs in using the given credentials.
	 *
	 * <p>
	 * The JesClient can store a JES spool owner. This constructor initializes the
	 * JES spool owner using the given username.
	 *
	 * <p>
	 * The default port is {@link org.apache.commons.net.ftp.FTP#DEFAULT_PORT}.
	 *
	 * <p>
	 * <b>Warning:</b> This constructor calls the overridable method
	 * {@link #login(String, String)}, which might lead to uninitialized fields when
	 * overriding that method.
	 *
	 * @param hostname FTP hostname
	 * @param port     FTP port
	 * @param username FTP username and JES spool owner
	 * @param password FTP password
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	@SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
	@SuppressJacocoGenerated(justification = "this constructor cannot be mocked nicely")
	@SuppressFBWarnings(value = "PCOA_PARTIALLY_CONSTRUCTED_OBJECT_ACCESS", justification = "see JavaDoc")
	public JesClient(final String hostname, final int port, final String username, final String password)
			throws IOException, JesException {
		this();
		ftpClient.connect(hostname, port);
		login(username, password);
	}

	/**
	 * Logs out and disconnects the FTP connection.
	 */
	@Override
	public void close() throws IOException {
		try {
			if (getFtpClient().isAvailable()) {
				getFtpClient().logout();
			}
		} finally {
			if (getFtpClient().isConnected()) {
				getFtpClient().disconnect();
			}
		}
	}

	/**
	 * Removes a given {@code job} from JES spool. This method cares only about the
	 * jobs ID.
	 *
	 * <p>
	 * In case you do not already have a {@link Job} object, deleting by job ID
	 * works as follows:
	 *
	 * <pre>
	 * String jobId = ...;
	 * jesClient.delete(new Job(jobId, JesClient.FILTER_WILDCARD, JobStatus.ALL, JesClient.FILTER_WILDCARD));
	 * </pre>
	 *
	 * @param job Job to be deleted
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public void delete(final Job job) throws IOException, JesException {
		if (!getFtpClient().deleteFile(job.getId())) {
			throw new JesException(getFtpClient(), "Job [%s] could not be deleted.", job.getId());
		}
	}

	/**
	 * Enters the IBM z/OS FTP servers JES file type mode using a SITE command.
	 *
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public void enterJesMode() throws IOException, JesException {
		if (!getFtpClient().sendSiteCommand("FILEtype=JES")) {
			throw new JesException(getFtpClient(), "Failed setting JES mode.");
		}
	}

	/**
	 * Reloads the job from server and returns {@code true} if the job is still
	 * available and matches the given job status.
	 *
	 * @param job    the job to search for
	 * @param status job status or ALL
	 * @return {@code true} if the job is still available
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public boolean exists(final Job job, final JobStatus status) throws IOException, JesException {
		setJesFilters(job.getName(), status, job.getOwner(), LIST_LIMIT_EXISTS);

		final String[] ids = getListNameResults(getFtpClient().listNames(job.getId()))
				.orElseThrow(() -> new JesException(getFtpClient(),
						"Retrieving job [%s] failed. Probably no FTP data connection socket could be opened.",
						job.getId()));
		return Optionals.ofSingle(ids).isPresent();
	}

	/**
	 * Retrieves up-to-date job details for {@code job}. That includes all
	 * {@link Job} attributes, including a list of {@link JobOutput} instances for
	 * held jobs.
	 *
	 * @param job job to get up-to-date details for
	 * @return job details or {@link Optional#empty()} in case the job is no longer
	 *         available inside JES spool
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public Optional<Job> getJobDetails(final Job job) throws IOException, JesException {
		setJesFilters(job.getName(), JobStatus.ALL, job.getOwner(), LIST_LIMIT_MAX);

		return Optionals
				.ofSingle(stream(getFtpClient().listFiles(job.getId())).filter(file -> file instanceof JesFtpFile)
						.map(file -> (JesFtpFile) file)
						.map(JesFtpFile::getJob));
	}

	/**
	 * Corrects the result of {@link FTPClient#listNames()} and
	 * {@link FTPClient#listNames(String)} as the mainframe FTP server marks empty
	 * name listings as error.
	 *
	 * @param names result of {@link FTPClient#listNames()} and
	 *              {@link FTPClient#listNames(String)}
	 * @return array of names or {@link Optional#empty()} on real FTP error
	 */
	@SuppressWarnings("PMD.UseVarargs")
	@SuppressFBWarnings(value = "UVA_USE_VAR_ARGS",
			justification = "No varargs needed as this is for special technical reasons only.")
	private Optional<String[]> getListNameResults(@Nullable final String[] names) {
		if (names == null) {
			return Patterns.find(PATTERN_LIST_NAMES_NO_JOBS_FOUND, getFtpClient().getReplyString())
					.map(matcher -> new String[0]);
		}
		return Optional.of(names);
	}

	/**
	 * Retrieves and parses a map of server properties, such as
	 * {@code "JESJOBNAME"}, {@code "JESSTATUS"}, {@code "JESOWNER"} and
	 * {@code "INTERFACELEVEL"}.
	 *
	 * @return map of server properties
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public Map<String, String> getServerProperties() throws IOException, JesException {
		// Execute STAT command
		if (!FTPReply.isPositiveCompletion(getFtpClient().stat())) {
			throw new JesException(getFtpClient(), "Failed executing STAT command.");
		}
		final String[] lines = getFtpClient().getReplyStrings();

		// Parse reply strings
		final Map<String, String> properties = new LinkedHashMap<>();
		for (final String line : lines) {
			final Optional<Matcher> matcher = Patterns.matches(PATTERN_STATUS, line);
			if (matcher.isPresent()) {
				// Key
				final String key = matcher.get().group("key");
				if (properties.containsKey(key)) {
					throw new JesException("Found duplicate status key \"%s\".", key);
				}

				// Value
				properties.put(key, matcher.get().group("value"));
			}
		}
		return properties;
	}

	/**
	 * Returns a list of all job IDs boxed into {@link Job} objects matching the
	 * given filters. This method has a much higher performance compared to
	 * {@link #listFilled(String)}, though that method fills in additional
	 * {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} is allowed to end with the wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter filter by job names
	 * @return list of jobs containing job IDs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> list(final String nameFilter) throws IOException, JesException {
		return list(nameFilter, JobStatus.ALL);
	}

	/**
	 * Returns a list of all job IDs boxed into {@link Job} objects matching the
	 * given filters. This method has a much higher performance compared to
	 * {@link #listFilled(String, JobStatus)}, though that method fills in
	 * additional {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} is allowed to end with the wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter filter by job names
	 * @param status     filter by job status
	 * @return list of jobs containing job IDs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> list(final String nameFilter, final JobStatus status) throws IOException, JesException {
		return list(nameFilter, status, FILTER_WILDCARD);
	}

	/**
	 * Returns a list of all job IDs boxed into {@link Job} objects matching the
	 * given filters. This method has a much higher performance compared to
	 * {@link #listFilled(String, JobStatus, String)}, though that method fills in
	 * additional {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} and {@code ownerFilter} are allowed to end with the
	 * wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter  filter by job names
	 * @param status      filter by job status
	 * @param ownerFilter filter by job owner
	 * @return list of jobs containing job IDs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> list(final String nameFilter, final JobStatus status, final String ownerFilter)
			throws IOException, JesException {
		return list(nameFilter, status, ownerFilter, LIST_LIMIT_MAX);
	}

	/**
	 * Returns a list of all job IDs boxed into {@link Job} objects matching the
	 * given filters. This method has a much higher performance compared to
	 * {@link #listFilled(String, JobStatus, String, int)}, though that method fills
	 * in additional {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} and {@code ownerFilter} are allowed to end with the
	 * wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@code limit} entries. In case more entries are
	 * available, a {@link JesLimitReachedException} is thrown, containing all
	 * entries up to the limit. {@code limit} can be from {@link #LIST_LIMIT_MIN}
	 * (including) to {@link #LIST_LIMIT_MAX} (including).
	 *
	 * @param nameFilter  filter by job names
	 * @param status      filter by job status
	 * @param ownerFilter filter by job owner
	 * @param limit       limit of spool entries
	 * @return list of jobs containing job IDs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> list(final String nameFilter, final JobStatus status, final String ownerFilter, final int limit)
			throws IOException, JesException {
		setJesFilters(nameFilter, status, ownerFilter, limit);

		final String[] ids = getListNameResults(getFtpClient().listNames()).orElseThrow(() -> new JesException(
				getFtpClient(),
				"Retrieving the list of job IDs failed. Probably no FTP data connection socket could be opened."));

		return throwIfLimitReached(limit,
				stream(ids).map(id -> new Job(id, nameFilter, status, ownerFilter)).collect(toList()));
	}

	/**
	 * Returns a list of all {@link Job} objects matching the given filters. This
	 * method has a worse performance compared to {@link #list(String)}, though it
	 * fills in additional {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} is allowed to end with the wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter filter by job names
	 * @return list of jobs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> listFilled(final String nameFilter) throws IOException, JesException {
		return listFilled(nameFilter, JobStatus.ALL);
	}

	/**
	 * Returns a list of all {@link Job} objects matching the given filters. This
	 * method has a worse performance compared to {@link #list(String, JobStatus)},
	 * though it fills in additional {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} is allowed to end with the wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter filter by job names
	 * @param status     filter by job status
	 * @return list of jobs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> listFilled(final String nameFilter, final JobStatus status) throws IOException, JesException {
		return listFilled(nameFilter, status, FILTER_WILDCARD);
	}

	/**
	 * Returns a list of all {@link Job} objects matching the given filters. This
	 * method has a worse performance compared to
	 * {@link #list(String, JobStatus, String)}, though it fills in additional
	 * {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} and {@code ownerFilter} are allowed to end with the
	 * wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter  filter by job names
	 * @param status      filter by job status
	 * @param ownerFilter filter by job owner
	 * @return list of jobs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> listFilled(final String nameFilter, final JobStatus status, final String ownerFilter)
			throws IOException, JesException {
		return listFilled(nameFilter, status, ownerFilter, LIST_LIMIT_MAX);
	}

	/**
	 * Returns a list of all {@link Job} objects matching the given filters. This
	 * method has a worse performance compared to
	 * {@link #list(String, JobStatus, String)}, though it fills in additional
	 * {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} and {@code ownerFilter} are allowed to end with the
	 * wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@code limit} entries. In case more entries are
	 * available, a {@link JesLimitReachedException} is thrown, containing all
	 * entries up to the limit. {@code limit} can be from {@link #LIST_LIMIT_MIN}
	 * (including) to {@link #LIST_LIMIT_MAX} (including).
	 *
	 * @param nameFilter  filter by job names
	 * @param status      filter by job status
	 * @param ownerFilter filter by job owner
	 * @param limit       limit of spool entries
	 * @return list of jobs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> listFilled(final String nameFilter,
			final JobStatus status,
			final String ownerFilter,
			final int limit) throws IOException, JesException {
		setJesFilters(nameFilter, status, ownerFilter, limit);

		final FTPFile[] files = getFtpClient().listFiles();

		return throwIfLimitReached(limit,
				stream(files).filter(file -> file instanceof JesFtpFile)
						.map(file -> (JesFtpFile) file)
						.map(JesFtpFile::getJob)
						.collect(toList()));
	}

	/**
	 * Shortcut method to perform a FTP login, set the internal JES owner and enter
	 * JES mode.
	 *
	 * <p>
	 * Is similar to the following lines of code.
	 *
	 * <pre>
	 * // Login via FTP client
	 * jesClient.getFtpClient().login(...);
	 *
	 * // Set the JES spool owner
	 * jesClient.setJesOwner(...);
	 *
	 * // Enter JES mode of the FTP connection
	 * jesClient.enterJesMode();
	 * </pre>
	 *
	 * @param username the user id to be used for FTP login and internal JES owner
	 * @param password the users password
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public void login(final String username, final String password) throws IOException, JesException {
		if (!getFtpClient().login(username, password)) {
			throw new JesException(getFtpClient(), "Could not login user [%s].", username);
		}
		setJesOwner(username);
		enterJesMode();
	}

	/**
	 * Retrieves the content of {@code jobOutput}.
	 *
	 * @param jobOutput job output to be requested
	 * @return content of the specified job output
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public String retrieve(final JobOutput jobOutput) throws IOException, JesException {
		final String fileName = Strings.format("%s.%d", jobOutput.getJob().getId(), jobOutput.getIndex());

		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			if (!getFtpClient().retrieveFile(fileName, outputStream)) {
				throw new JesException(getFtpClient(),
						"Could not retrieve data of job output [%s.%s].",
						jobOutput.getJob().getId(),
						jobOutput.getStep());
			}
			return new String(outputStream.toByteArray(), FTP_DATA_CHARSET);
		}
	}

	/**
	 * Retrieves all job outputs of {@code job}.
	 *
	 * @param job job to request all outputs of
	 * @return map with job output details and the corresponding content in specific
	 *         order
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public Map<JobOutput, String> retrieveOutputs(final Job job) throws IOException, JesException {
		if (job.getOutputs().isEmpty()) {
			return retrieveOutputs(
					getJobDetails(job).orElseThrow(() -> new JesException("Job [%s] is not available.", job.getId())));
		}
		return job.getOutputs().stream().collect(toLinkedHashMap(Function.identity(), throwing(this::retrieve)));
	}

	/**
	 * Sends {@link org.apache.commons.net.ftp.FTPCmd#SITE} commands to set the
	 * given filter values.
	 *
	 * <p>
	 * {@code nameFilter} and {@code ownerFilter} are allowed to end with the
	 * wildcard character "*".
	 *
	 * <p>
	 * {@code limit} can be from {@link #LIST_LIMIT_MIN} (including) to
	 * {@link #LIST_LIMIT_MAX} (including). While that restriction is not checked by
	 * this method, values outside that range might result in a server side error
	 * message thrown as {@link JesException}.
	 *
	 * @param nameFilter  filter by job names
	 * @param status      filter by job status
	 * @param ownerFilter filter by job owner
	 * @param limit       limit of spool entries
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	protected void setJesFilters(final String nameFilter,
			final JobStatus status,
			final String ownerFilter,
			final int limit) throws IOException, JesException {
		if (!getFtpClient().sendSiteCommand("JESJOBName=" + nameFilter)) {
			throw new JesException(getFtpClient(), "Failed setting JES job name filter to [%s].", nameFilter);
		}
		if (!getFtpClient().sendSiteCommand("JESOwner=" + ownerFilter)) {
			throw new JesException(getFtpClient(), "Failed setting JES job owner filter to [%s].", ownerFilter);
		}
		if (!getFtpClient().sendSiteCommand("JESSTatus=" + status.getValue())) {
			throw new JesException(getFtpClient(), "Failed setting JES job status filter to [%s].", status.getValue());
		}
		if (!getFtpClient().sendSiteCommand("JESENTRYLIMIT=" + limit)) {
			throw new JesException(getFtpClient(),
					"Failed setting JES entry limit to %d. Minimum/Maximum: %d/%d",
					limit,
					LIST_LIMIT_MIN,
					LIST_LIMIT_MAX);
		}
	}

	/**
	 * Current JES spool user
	 *
	 * @param jesOwner JES spool owner
	 */
	public void setJesOwner(final String jesOwner) {
		this.jesOwner = Strings.toUpperCaseNeutral(jesOwner).trim();
	}

	/**
	 * Submits the given JCL and returns a related {@link Job} object containing the
	 * started jobs ID.
	 *
	 * <p>
	 * In addition to the jobs ID this method tries to extract the jobs name from
	 * the given JCL. The returned owner is set to the internal JES owner, which can
	 * be set using {@link #setJesOwner(String)}.
	 *
	 * @param jclContent JCL to submit
	 * @return {@link Job} object containing the started jobs ID
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public Job submit(final String jclContent) throws IOException, JesException {
		try (InputStream inputStream = new ReaderInputStream(new StringReader(jclContent), FTP_DATA_CHARSET)) {
			if (!getFtpClient().storeUniqueFile(SUBMIT_REMOTE_FILE_NAME, inputStream)) {
				throw new JesException(getFtpClient(), "Submitting JCL failed.");
			}
		}

		final String jobId = Patterns.find(PATTERN_FTP_SUBMIT_ID, getFtpClient().getReplyString())
				.map(matcher -> matcher.group("id"))
				.orElseThrow(() -> new JesException(getFtpClient(), "Started job, but could not extract its ID."));
		final String name = Patterns.find(PATTERN_JCL_JOB_NAME, jclContent)
				.map(matcher -> matcher.group("name"))
				.orElse(FILTER_WILDCARD);

		return new Job(jobId, name, JobStatus.INPUT, getJesOwner());
	}

	/**
	 * In case the last FTP responses string contains the spool entries limit
	 * warning, a {@link JesLimitReachedException} is thrown, else {@code jobs} are
	 * returned.
	 *
	 * <p>
	 * The thrown exception contains the current spool entries limit and all
	 * entries, which were read already.
	 *
	 * @param limit current spool entries limit
	 * @param jobs  list of jobs
	 * @return {@code jobs} in case the spool entries limit is not reached
	 * @throws JesLimitReachedException if the last FTP responses string contains
	 *                                  the spool entries limit warning
	 */
	protected List<Job> throwIfLimitReached(final int limit, final List<Job> jobs) throws JesLimitReachedException {
		if (Strings.find(getFtpClient().getReplyString(), PATTERN_LIST_LIMIT)) {
			throw new JesLimitReachedException(limit, jobs, getFtpClient());
		}
		return jobs;
	}

	/**
	 * Waits for {@code job} to be finished using {@code Thread#sleep(long)} for
	 * waiting between {@link #exists(Job, JobStatus)} calls and timing out after a
	 * given duration. {@code waiting} allows to specify the duration to wait.
	 *
	 * <p>
	 * The given jobs status specifies, which status are waited for:
	 * <ul>
	 * <li>{@link JobStatus#ALL}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#INPUT}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#ACTIVE}: waiting for {@link JobStatus#ACTIVE} only
	 * <li>{@link JobStatus#OUTPUT}: returning {@code true} with no checks and
	 * without waiting
	 * </ul>
	 *
	 * @param job     the job to wait for
	 * @param waiting duration to wait
	 * @param timeout timeout duration
	 * @return {@code true} if the job finished and {@code false} if the timeout has
	 *         been reached
	 * @throws InterruptedException if any thread has interrupted the current thread
	 * @throws IOException          Technical FTP failure
	 * @throws JesException         Logical JES failure
	 */
	@SuppressWarnings("unused")
	public boolean waitFor(final Job job, final Duration waiting, final Duration timeout)
			throws InterruptedException, IOException, JesException {
		return waitFor(job,
				waiting,
				timeout,
				ThrowingConsumer.throwing(duration -> Thread.sleep(Nullables.orElseThrow(duration).toMillis())));
	}

	/**
	 * Waits for {@code job} to be finished using {@code wait} for waiting between
	 * {@link #exists(Job, JobStatus)} calls and timing out after a given duration.
	 * {@code waiting} allows to specify the duration to wait.
	 *
	 * <p>
	 * The given jobs status specifies, which status are waited for:
	 * <ul>
	 * <li>{@link JobStatus#ALL}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#INPUT}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#ACTIVE}: waiting for {@link JobStatus#ACTIVE} only
	 * <li>{@link JobStatus#OUTPUT}: returning {@code true} with no checks and
	 * without waiting
	 * </ul>
	 *
	 * @param job     the job to wait for
	 * @param waiting duration to wait
	 * @param timeout timeout duration
	 * @param wait    method to use for waiting
	 * @return {@code true} if the job finished and {@code false} if the timeout has
	 *         been reached
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public boolean waitFor(final Job job, final Duration waiting, final Duration timeout, final Consumer<Duration> wait)
			throws IOException, JesException {
		if (job.getStatus() == JobStatus.OUTPUT) {
			return true;
		}

		// Status INPUT and ACTIVE might need to be waited for
		final List<JobStatus> stati = job.getStatus() == JobStatus.ACTIVE
				? singletonList(JobStatus.ACTIVE)
				: asList(JobStatus.INPUT, JobStatus.ACTIVE);

		// Waiting for the status
		final Stopwatch stopwatch = new Stopwatch();
		for (final JobStatus status : stati) {
			while (exists(job, status)) {
				if (!stopwatch.waitFor(waiting, timeout, wait)) {
					return false;
				}
			}
		}
		return true;
	}
}
