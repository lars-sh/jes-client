package de.larssh.jes.parser;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import de.larssh.utils.SneakyException;
import lombok.NoArgsConstructor;

/**
 * {@link JesFtpFileEntryParser}
 */
@NoArgsConstructor
public final class JesFtpFileEntryParserTest {
	private final static JesFtpFileEntryParser INSTANCE = new JesFtpFileEntryParser();

	private final static Map<String, Integer> PRE_PARSE_EXPECTED_SIZES;

	static {
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
	@SuppressWarnings("checkstyle:TodoComment")
	public void testParseFTPEntry() {
		assertThrows(IllegalArgumentException.class, () -> INSTANCE.parseFTPEntry(null));
		assertThrows(JesFtpFileEntryParserException.class, () -> INSTANCE.parseFTPEntry(""));
		assertThrows(JesFtpFileEntryParserException.class, () -> INSTANCE.parseFTPEntry(" "));

		// TODO
	}

	/**
	 * {@link JesFtpFileEntryParser#preParse(java.util.List)}
	 */
	@Test
	public void testPreParse() {
		assertThrows(IllegalArgumentException.class, () -> INSTANCE.preParse(null));
		assertThrows(JesFtpFileEntryParserException.class, () -> INSTANCE.preParse(emptyList()));
		assertThrows(JesFtpFileEntryParserException.class, () -> INSTANCE.preParse(Arrays.asList(" ")));

		try {
			try (final Stream<Path> paths
					= Files.list(Paths.get(getClass().getResource(getClass().getSimpleName() + ".preParse").toURI()))) {
				paths.filter(Files::isRegularFile).forEach(path -> {
					final int expectedSize = Optional.ofNullable(path.getFileName())
							.map(fileName -> PRE_PARSE_EXPECTED_SIZES.get(fileName.toString()))
							.orElseThrow(() -> new IllegalArgumentException(
									String.format("Missing expected size for file [%s].", path)));

					try (final BufferedReader reader = Files.newBufferedReader(path)) {
						assertEquals(expectedSize, INSTANCE.preParse(reader.lines().collect(toList())).size());
					} catch (final IOException e) {
						throw new UncheckedIOException(e);
					}
				});
			}
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final URISyntaxException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesFtpFileEntryParser#readNextEntry(BufferedReader)}
	 */
	@Test
	public void testReadNextEntry() {
		try {
			assertThrows(IllegalArgumentException.class, () -> INSTANCE.readNextEntry(null));
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
