package de.larssh.jes.parser;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import de.larssh.jes.Job;
import de.larssh.jes.JobFlag;
import de.larssh.jes.JobStatus;
import de.larssh.utils.SneakyException;
import de.larssh.utils.collection.Maps;
import lombok.NoArgsConstructor;

/**
 * {@link JesFtpFileEntryParser}
 */
@NoArgsConstructor
public final class JesFtpFileEntryParserTest {
	private static final JesFtpFileEntryParser INSTANCE = new JesFtpFileEntryParser();

	private static final Path PATH_FTP_INPUT;

	private static final Path PATH_FTP_INPUT_THROWS;

	// @formatter:off
	private static final Map<String, List<Job>> PARSE_FTP_ENTRY_EXPECTED_JOBS = Maps.builder(new LinkedHashMap<String, List<Job>>())
			.put("abend.txt", asList(
				new Job("JOB00009", "JABC456", JobStatus.OUTPUT, "USER9", Optional.of("I"), OptionalInt.empty(), Optional.of("622")),
				new Job("JOB00010", "JABC789", JobStatus.OUTPUT, "USER10", Optional.of("J"), OptionalInt.empty(), Optional.of("EC6")),
				new Job("TSU08743", "USER2", JobStatus.OUTPUT, "USER2", Optional.of("TSU"), OptionalInt.empty(), Optional.of("622"))))
			.put("dup.txt", singletonList(
				new Job("JOB00003", "JABC678", JobStatus.INPUT, "USER3", Optional.of("C"), OptionalInt.empty(), Optional.empty(), JobFlag.DUP)))
			.put("held.txt", singletonList(
				new Job("JOB00002", "JABC345", JobStatus.INPUT, "USER2", Optional.of("B"), OptionalInt.empty(), Optional.empty(), JobFlag.HELD)))
			.put("jcl-error.txt", singletonList(
				new Job("JOB00008", "JABC123", JobStatus.OUTPUT, "USER8", Optional.of("H"), OptionalInt.empty(), Optional.empty(), JobFlag.JCL_ERROR)))
			.put("not-accessible.txt", asList(
				new Job("STC85256", "ABCDEF3", JobStatus.OUTPUT, "USER3", Optional.of("STC"), OptionalInt.of(0), Optional.empty(), JobFlag.HELD),
				new Job("STC21743", "ABCDEF4", JobStatus.OUTPUT, "USER4", Optional.of("STC"), OptionalInt.empty(), Optional.empty(), JobFlag.HELD)))
			.put("rc.txt", asList(
				new Job("JOB00005", "JABC234", JobStatus.OUTPUT, "USER5", Optional.of("E"), OptionalInt.of(0), Optional.empty()),
				new Job("JOB00006", "JABC567", JobStatus.OUTPUT, "USER9", Optional.of("F"), OptionalInt.of(1), Optional.empty()),
				new Job("JOB00007", "JABC890", JobStatus.OUTPUT, "USER7", Optional.of("G"), OptionalInt.empty(), Optional.empty()),
				new Job("STC18403", "ABCDEF2", JobStatus.OUTPUT, "USER2", Optional.of("STC"), OptionalInt.of(2), Optional.empty()),
				new Job("TSU15944", "USER3", JobStatus.OUTPUT, "USER3", Optional.of("TSU"), OptionalInt.of(3), Optional.empty())))
			.put("simple.txt", singletonList(
				new Job("JOB00001", "JABC012", JobStatus.INPUT, "USER1", Optional.of("A"), OptionalInt.empty(), Optional.empty())))
			.put("job-output-byte-count.txt", singletonList(
				new Job("JOB00054", "USER1", JobStatus.OUTPUT, "USER1", Optional.of("A"), OptionalInt.of(0), Optional.empty())))
			.put("job-output-empty.txt", singletonList(
				new Job("JOB00054", "USER1", JobStatus.OUTPUT, "USER1", Optional.of("A"), OptionalInt.of(0), Optional.empty())))
			.put("job-output-rec-count.txt", singletonList(
				new Job("JOB00061", "USER3A", JobStatus.OUTPUT, "USER3", Optional.of("D"), OptionalInt.of(0), Optional.empty())))
			.unmodifiable();
	// @formatter:on

	private static final Set<String> PARSE_FTP_ENTRY_THROWS_EXPECTED_JOBS
			= Collections.unmodifiableSet(new LinkedHashSet<>(asList("sub-title.txt", "job-output.txt")));

	private static final Map<String, Integer> PRE_PARSE_EXPECTED_SIZES
			= Maps.builder(new LinkedHashMap<String, Integer>())
					.put("abend.txt", 3)
					.put("dup.txt", 1)
					.put("held.txt", 1)
					.put("jcl-error.txt", 1)
					.put("not-accessible.txt", 2)
					.put("rc.txt", 5)
					.put("simple.txt", 1)
					.unmodifiable();

	static {
		try {
			PATH_FTP_INPUT = Paths.get(JesFtpFileEntryParserTest.class.getResource("ftp-input").toURI());
		} catch (final URISyntaxException e) {
			throw new SneakyException(e);
		}
		PATH_FTP_INPUT_THROWS = PATH_FTP_INPUT.resolve("throws");

		// Outputs
		final Job jobOutputByteCount = PARSE_FTP_ENTRY_EXPECTED_JOBS.get("job-output-byte-count.txt").get(0);
		jobOutputByteCount.createOutput(1, "JESMSGLG", 1200, Optional.of("JESE"), Optional.empty(), Optional.of("H"));
		jobOutputByteCount.createOutput(2, "JESJCL", 526, Optional.of("JESE"), Optional.empty(), Optional.of("H"));
		jobOutputByteCount.createOutput(3, "JESYSMSG", 1255, Optional.of("JESE"), Optional.empty(), Optional.of("H"));
		jobOutputByteCount.createOutput(4, "SYSUT2", 741, Optional.of("STEP57"), Optional.empty(), Optional.of("H"));
		jobOutputByteCount.createOutput(5, "SYSPRINT", 209, Optional.of("STEP57"), Optional.empty(), Optional.of("A"));

		final Job jobOutputRecCount = PARSE_FTP_ENTRY_EXPECTED_JOBS.get("job-output-rec-count.txt").get(0);
		jobOutputRecCount.createOutput(1, "JESMSGLG", 18, Optional.of("JESE"), Optional.empty(), Optional.of("H"));
		jobOutputRecCount.createOutput(2, "JESJCL", 11, Optional.of("JESE"), Optional.empty(), Optional.of("H"));
		jobOutputRecCount.createOutput(3, "JESYSMSG", 22, Optional.empty(), Optional.empty(), Optional.of("A"));
	}

	/**
	 * Assert that two lists of {@link Job} are equal. If necessary, the failure
	 * message will be retrieved lazily from {@code messageSupplier}.
	 *
	 * <p>
	 * This method considers two {@code Job} objects as equal if all of their fields
	 * are equal. That behavior varies from {@link Job#equals(Object)}!
	 *
	 * <p>
	 * <b>Implementation Notice:</b> {@link Job#toString()} serializes all fields
	 * and therefore does a good job for comparing equality for tests. This code
	 * should not be used for production systems.
	 *
	 * @param expected        the expected list
	 * @param actual          the actual list
	 * @param messageSupplier the supplier to retrieve a failure message lazily
	 */
	private static void assertEqualsJobList(final List<Job> expected,
			final List<Job> actual,
			final Supplier<String> messageSupplier) {
		assertEquals(expected.toString(), actual.toString(), messageSupplier);
	}

	/**
	 * {@link JesFtpFileEntryParser#parseFTPEntry(String)}
	 */
	@Test
	public void testParseFTPEntry() {
		assertThrows(NullPointerException.class, () -> INSTANCE.parseFTPEntry(null));
		assertThrows(JesFtpFileEntryParserException.class, () -> INSTANCE.parseFTPEntry(""));
		assertThrows(JesFtpFileEntryParserException.class, () -> INSTANCE.parseFTPEntry(" "));

		for (final Entry<String, List<Job>> entry : PARSE_FTP_ENTRY_EXPECTED_JOBS.entrySet()) {
			final Path path = PATH_FTP_INPUT.resolve(entry.getKey());
			try (BufferedReader reader = Files.newBufferedReader(path)) {
				assertEqualsJobList(entry.getValue(),
						INSTANCE.preParse(reader.lines().collect(toList()))
								.stream()
								.map(INSTANCE::parseFTPEntry)
								.map(JesFtpFile::getJob)
								.collect(toList()),
						() -> path.toString());
			} catch (final IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		// throws
		for (final String fileName : PARSE_FTP_ENTRY_THROWS_EXPECTED_JOBS) {
			final Path path = PATH_FTP_INPUT_THROWS.resolve(fileName);
			try (BufferedReader reader = Files.newBufferedReader(path)) {
				assertThrows(JesFtpFileEntryParserException.class,
						() -> INSTANCE.preParse(reader.lines().collect(toList())).forEach(INSTANCE::parseFTPEntry));
			} catch (final IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	/**
	 * {@link JesFtpFileEntryParser#preParse(java.util.List)}
	 */
	@Test
	public void testPreParse() {
		assertThrows(NullPointerException.class, () -> INSTANCE.preParse(null));
		assertThrows(JesFtpFileEntryParserException.class, () -> INSTANCE.preParse(emptyList()));
		assertThrows(JesFtpFileEntryParserException.class, () -> INSTANCE.preParse(Arrays.asList(" ")));

		for (final Entry<String, Integer> entry : PRE_PARSE_EXPECTED_SIZES.entrySet()) {
			final Path path = PATH_FTP_INPUT.resolve(entry.getKey());

			try (BufferedReader reader = Files.newBufferedReader(path)) {
				assertEquals(entry.getValue(),
						INSTANCE.preParse(reader.lines().collect(toList())).size(),
						() -> path.toString());
			} catch (final IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	/**
	 * {@link JesFtpFileEntryParser#readNextEntry(BufferedReader)}
	 */
	@Test
	public void testReadNextEntry() {
		try {
			assertThrows(NullPointerException.class, () -> INSTANCE.readNextEntry(null));
			assertNull(INSTANCE.readNextEntry(new BufferedReader(new StringReader(""))));

			try (BufferedReader reader = new BufferedReader(new StringReader("\n"))) {
				assertEquals("", INSTANCE.readNextEntry(reader));
				assertNull(INSTANCE.readNextEntry(reader));
			}
			try (BufferedReader reader = new BufferedReader(new StringReader("\r\n"))) {
				assertEquals("", INSTANCE.readNextEntry(reader));
				assertNull(INSTANCE.readNextEntry(reader));
			}
			try (BufferedReader reader = new BufferedReader(new StringReader("\r"))) {
				assertEquals("", INSTANCE.readNextEntry(reader));
				assertNull(INSTANCE.readNextEntry(reader));
			}

			try (BufferedReader reader = new BufferedReader(new StringReader("a"))) {
				assertEquals("a", INSTANCE.readNextEntry(reader));
				assertNull(INSTANCE.readNextEntry(reader));
			}
			try (BufferedReader reader = new BufferedReader(new StringReader("a\n"))) {
				assertEquals("a", INSTANCE.readNextEntry(reader));
				assertNull(INSTANCE.readNextEntry(reader));
			}
			try (BufferedReader reader = new BufferedReader(new StringReader("a\nb"))) {
				assertEquals("a", INSTANCE.readNextEntry(reader));
				assertEquals("b", INSTANCE.readNextEntry(reader));
				assertNull(INSTANCE.readNextEntry(reader));
			}
			try (BufferedReader reader = new BufferedReader(new StringReader("a\n\nb"))) {
				assertEquals("a", INSTANCE.readNextEntry(reader));
				assertEquals("", INSTANCE.readNextEntry(reader));
				assertEquals("b", INSTANCE.readNextEntry(reader));
				assertNull(INSTANCE.readNextEntry(reader));
			}
			try (BufferedReader reader = new BufferedReader(new StringReader("a\r\nb"))) {
				assertEquals("a", INSTANCE.readNextEntry(reader));
				assertEquals("b", INSTANCE.readNextEntry(reader));
				assertNull(INSTANCE.readNextEntry(reader));
			}
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
