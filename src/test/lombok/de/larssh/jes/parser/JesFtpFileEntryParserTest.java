package de.larssh.jes.parser;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import de.larssh.jes.Job;
import de.larssh.jes.JobFlag;
import de.larssh.jes.JobStatus;
import de.larssh.utils.Nullables;
import de.larssh.utils.SneakyException;
import lombok.NoArgsConstructor;

/**
 * {@link JesFtpFileEntryParser}
 */
@NoArgsConstructor
public final class JesFtpFileEntryParserTest {
	private static final JesFtpFileEntryParser INSTANCE = new JesFtpFileEntryParser();

	private static final Path PATH_FTP_INPUT;

	private static final Map<String, List<Job>> PARSE_FTP_ENTRY_EXPECTED_JOBS;

	private static final Map<String, Integer> PRE_PARSE_EXPECTED_SIZES;

	static {
		try {
			PATH_FTP_INPUT = Paths.get(JesFtpFileEntryParserTest.class.getResource("ftp-input").toURI());
		} catch (final URISyntaxException e) {
			throw new SneakyException(e);
		}

		// @formatter:off
		final Map<String, List<Job>> parseFtpEntryExpectedJobs = new HashMap<>();
		parseFtpEntryExpectedJobs.put("abend.txt", asList(
				new Job("JOB00009", "JABC456", JobStatus.OUTPUT, "USER9", Optional.of("I"), OptionalInt.empty(), Optional.of("622")),
				new Job("JOB00010", "JABC789", JobStatus.OUTPUT, "USER10", Optional.of("J"), OptionalInt.empty(), Optional.of("EC6")),
				new Job("TSU08743", "JABC456", JobStatus.OUTPUT, "USER2", Optional.of("TSU"), OptionalInt.empty(), Optional.of("622"))));
		parseFtpEntryExpectedJobs.put("dup.txt", singletonList(
				new Job("JOB00003", "JABC678", JobStatus.INPUT, "USER3", Optional.of("C"), OptionalInt.empty(), Optional.empty(), JobFlag.DUP)));
		parseFtpEntryExpectedJobs.put("held.txt", singletonList(
				new Job("JOB00002", "JABC345", JobStatus.INPUT, "USER2", Optional.of("B"), OptionalInt.empty(), Optional.empty(), JobFlag.HELD)));
		parseFtpEntryExpectedJobs.put("jcl-error.txt", singletonList(
				new Job("JOB00008", "JABC123", JobStatus.OUTPUT, "USER8", Optional.of("H"), OptionalInt.empty(), Optional.empty(), JobFlag.JCL_ERROR)));
		parseFtpEntryExpectedJobs.put("not-accessible.txt", asList(
				new Job("STC85256", "ABCDEF3", JobStatus.OUTPUT, "USER3", Optional.of("STC"), OptionalInt.of(0), Optional.empty(), JobFlag.HELD),
				new Job("STC21743", "ABCDEF4", JobStatus.OUTPUT, "USER4", Optional.of("STC"), OptionalInt.empty(), Optional.empty(), JobFlag.HELD)));
		parseFtpEntryExpectedJobs.put("rc.txt", asList(
				new Job("JOB00005", "JABC234", JobStatus.OUTPUT, "USER5", Optional.of("E"), OptionalInt.of(0), Optional.empty()),
				new Job("JOB00006", "JABC567", JobStatus.OUTPUT, "USER9", Optional.of("F"), OptionalInt.of(1), Optional.empty()),
				new Job("JOB00007", "JABC890", JobStatus.OUTPUT, "USER7", Optional.of("G"), OptionalInt.empty(), Optional.empty()),
				new Job("STC18403", "ABCDEF2", JobStatus.OUTPUT, "USER2", Optional.of("STC"), OptionalInt.of(2), Optional.empty()),
				new Job("TSU15944", "USER3", JobStatus.OUTPUT, "USER3", Optional.of("TSU"), OptionalInt.of(3), Optional.empty())));
		parseFtpEntryExpectedJobs.put("simple.txt", singletonList(
				new Job("JOB00001", "JABC012", JobStatus.INPUT, "USER1", Optional.of("A"), OptionalInt.empty(), Optional.empty())));
		PARSE_FTP_ENTRY_EXPECTED_JOBS = unmodifiableMap(parseFtpEntryExpectedJobs);
		// @formatter:on

		final Map<String, Integer> preParseExpectedSizes = new HashMap<>();
		preParseExpectedSizes.put("abend.txt", 3);
		preParseExpectedSizes.put("dup.txt", 1);
		preParseExpectedSizes.put("held.txt", 1);
		preParseExpectedSizes.put("jcl-error.txt", 1);
		preParseExpectedSizes.put("not-accessible.txt", 2);
		preParseExpectedSizes.put("rc.txt", 5);
		preParseExpectedSizes.put("simple.txt", 1);
		PRE_PARSE_EXPECTED_SIZES = unmodifiableMap(preParseExpectedSizes);
	}

	/**
	 * {@link JesFtpFileEntryParser#parseFTPEntry(String)}
	 */
	@Test
	public void testParseFTPEntry() {
		assertThrows(NullPointerException.class, () -> INSTANCE.parseFTPEntry(null));
		assertThrows(JesFtpFileEntryParserException.class, () -> INSTANCE.parseFTPEntry(""));
		assertThrows(JesFtpFileEntryParserException.class, () -> INSTANCE.parseFTPEntry(" "));

		try (Stream<Path> paths = Files.list(PATH_FTP_INPUT)) {
			paths.filter(Files::isRegularFile).forEach(path -> {
				final Path fileName = Nullables.orElseThrow(path.getFileName(),
						() -> new IllegalArgumentException(
								String.format("Missing expected jobs for file [%s].", path)));

				try (BufferedReader reader = Files.newBufferedReader(path)) {
					assertEquals(PARSE_FTP_ENTRY_EXPECTED_JOBS.get(fileName.toString()),
							INSTANCE.preParse(reader.lines().collect(toList()))
									.stream()
									.map(INSTANCE::parseFTPEntry)
									.map(JesFtpFile::getJob)
									.collect(toList()),
							() -> path.toString());
				} catch (final IOException e) {
					throw new UncheckedIOException(e);
				}
			});
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
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

		try (Stream<Path> paths = Files.list(PATH_FTP_INPUT)) {
			paths.filter(Files::isRegularFile).forEach(path -> {
				final Path fileName = Nullables.orElseThrow(path.getFileName(),
						() -> new IllegalArgumentException(
								String.format("Missing expected size for file [%s].", path)));

				try (BufferedReader reader = Files.newBufferedReader(path)) {
					assertEquals(PRE_PARSE_EXPECTED_SIZES.get(fileName.toString()),
							INSTANCE.preParse(reader.lines().collect(toList())).size(),
							() -> path.toString());
				} catch (final IOException e) {
					throw new UncheckedIOException(e);
				}
			});
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
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
