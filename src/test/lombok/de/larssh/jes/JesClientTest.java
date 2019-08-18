package de.larssh.jes;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.joor.Reflect;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.Invocation;

import de.larssh.jes.parser.JesFtpFile;
import de.larssh.utils.Nullables;
import de.larssh.utils.SneakyException;
import de.larssh.utils.function.ThrowingConsumer;
import de.larssh.utils.test.Reflects;
import de.larssh.utils.time.Stopwatch;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.NoArgsConstructor;
import lombok.experimental.NonFinal;

/**
 * {@link JesClient}
 */
@NoArgsConstructor
@SuppressWarnings({
		"checkstyle:SuppressWarnings",
		"PMD.ExcessiveClassLength",
		"PMD.ExcessiveImports",
		"PMD.ExcessiveMethodLength",
		"PMD.NcssCount" })
public class JesClientTest {
	private static final Job TEST_DATA_JOB = new Job("id", "name", JobStatus.OUTPUT, "owner");

	/**
	 * Verify and clears a mocked JES client and its inner mocks for no more
	 * interactions.
	 *
	 * @param jesClient mocked JES Client to be verified
	 */
	private static void verifyEnd(final MockedJesClient jesClient) {
		for (final Invocation invocation : mockingDetails(jesClient).getInvocations()) {
			if (invocation.getMethod().getDeclaringClass() == MockedJesClient.class) {
				invocation.ignoreForVerification();
			}
		}

		verifyNoMoreInteractions(jesClient);
		verifyNoMoreInteractions(jesClient.getFtpClient());
		clearInvocations(jesClient, jesClient.getFtpClient());
	}

	/**
	 * Verify and clears a mocked JES client and its inner mocks for no more
	 * interactions. The last performed mocked action is expected to be the creation
	 * of a {@link JesException}.
	 *
	 * @param jesClient mocked JES Client to be verified
	 */
	private static void verifyEndWithJesException(final MockedJesClient jesClient) {
		verify(jesClient.getFtpClient()).getReplyString();
		verifyEnd(jesClient);
	}

	/**
	 * {@link JesClient#close()}
	 */
	@Test
	public void testClose() {
		// given
		try {
			@SuppressWarnings("resource")
			final MockedJesClient jesClient = MockedJesClient.newInstance();
			when(jesClient.getFtpClient().isAvailable()).thenReturn(true);
			when(jesClient.getFtpClient().isConnected()).thenReturn(true);

			// when
			jesClient.close();

			// then
			verify(jesClient).close();
			verify(jesClient.getFtpClient()).isAvailable();
			verify(jesClient.getFtpClient()).logout();
			verify(jesClient.getFtpClient()).isConnected();
			verify(jesClient.getFtpClient()).disconnect();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}

		// given
		try {
			@SuppressWarnings("resource")
			final MockedJesClient jesClient = MockedJesClient.newInstance();
			when(jesClient.getFtpClient().isAvailable()).thenReturn(false);
			when(jesClient.getFtpClient().isConnected()).thenReturn(true);

			// when
			jesClient.close();

			// then
			verify(jesClient).close();
			verify(jesClient.getFtpClient()).isAvailable();
			verify(jesClient.getFtpClient()).isConnected();
			verify(jesClient.getFtpClient()).disconnect();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}

		// given
		try {
			@SuppressWarnings("resource")
			final MockedJesClient jesClient = MockedJesClient.newInstance();
			when(jesClient.getFtpClient().isAvailable()).thenReturn(false);
			when(jesClient.getFtpClient().isConnected()).thenReturn(false);

			// when
			jesClient.close();

			// then
			verify(jesClient).close();
			verify(jesClient.getFtpClient()).isAvailable();
			verify(jesClient.getFtpClient()).isConnected();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/**
	 * {@link JesClient#delete(Job)}
	 */
	@Test
	public void testDelete() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().deleteFile(any())).thenReturn(true);

			// when
			jesClient.delete(TEST_DATA_JOB);

			// then
			verify(jesClient).delete(any());
			verify(jesClient.getFtpClient()).deleteFile("ID");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().deleteFile(any())).thenReturn(false);

			// when
			assertThrows(JesException.class, () -> jesClient.delete(TEST_DATA_JOB));

			// then
			verify(jesClient).delete(any());
			verify(jesClient.getFtpClient()).deleteFile("ID");
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#enterJesMode()}
	 */
	@Test
	public void testEnterJesMode() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(any())).thenReturn(true);

			// when
			jesClient.enterJesMode();

			// then
			verify(jesClient).enterJesMode();
			verify(jesClient.getFtpClient()).sendSiteCommand("FILEtype=JES");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(any())).thenReturn(false);

			// when
			assertThrows(JesException.class, () -> jesClient.enterJesMode());

			// then
			verify(jesClient).enterJesMode();
			verify(jesClient.getFtpClient()).sendSiteCommand("FILEtype=JES");
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#exists(Job, JobStatus)}
	 */
	@Test
	public void testExists() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames(any())).thenReturn(new String[0]);

			// when
			assertFalse(jesClient.exists(TEST_DATA_JOB, JobStatus.INPUT));

			// then
			verify(jesClient).exists(any(), any());
			verify(jesClient).setJesFilters("NAME", JobStatus.INPUT, "OWNER", 2);
			verify(jesClient.getFtpClient()).listNames("ID");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames(any())).thenReturn(new String[] { "ID" });

			// when
			assertTrue(jesClient.exists(TEST_DATA_JOB, JobStatus.INPUT));

			// then
			verify(jesClient).exists(any(), any());
			verify(jesClient).setJesFilters("NAME", JobStatus.INPUT, "OWNER", 2);
			verify(jesClient.getFtpClient()).listNames("ID");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames(any())).thenReturn(null);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("");

			// when
			assertThrows(JesException.class, () -> jesClient.exists(TEST_DATA_JOB, JobStatus.INPUT));

			// then
			verify(jesClient).exists(any(), any());
			verify(jesClient).setJesFilters("NAME", JobStatus.INPUT, "OWNER", 2);
			verify(jesClient.getFtpClient()).listNames("ID");
			verify(jesClient.getFtpClient(), times(2)).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames(any())).thenReturn(null);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("550 NO JOBS FOUND FOR ...");

			// when
			assertFalse(jesClient.exists(TEST_DATA_JOB, JobStatus.INPUT));

			// then
			verify(jesClient).exists(any(), any());
			verify(jesClient).setJesFilters("NAME", JobStatus.INPUT, "OWNER", 2);
			verify(jesClient.getFtpClient()).listNames("ID");
			verify(jesClient.getFtpClient()).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#getJesOwner()} and {@link JesClient#setJesOwner(String)}
	 */
	@Test
	@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
			justification = "Return value of Mockedverify(jesClient).getJesOwner() does not matter.")
	public void testGetAndSetJesOwner() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {

			// when 1
			assertEquals("*", jesClient.getJesOwner());

			// then 1
			verify(jesClient).getJesOwner();
			verifyEnd(jesClient);

			// when 2
			jesClient.setJesOwner("jesOwner1");
			assertEquals("JESOWNER1", jesClient.getJesOwner());

			// then 2
			verify(jesClient).setJesOwner(any());
			verify(jesClient).getJesOwner();
			verifyEnd(jesClient);

			// when 3
			jesClient.setJesOwner(" jesOwner2 ");
			assertEquals("JESOWNER2", jesClient.getJesOwner());

			// then 3
			verify(jesClient).setJesOwner(any());
			verify(jesClient).getJesOwner();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/**
	 * {@link JesClient#getJobDetails(Job)}
	 */
	@Test
	public void testGetJobDetails() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listFiles(any()))
					.thenReturn(new JesFtpFile[] { new JesFtpFile(TEST_DATA_JOB, "") });

			// when
			assertEquals(Optional.of(TEST_DATA_JOB), jesClient.getJobDetails(TEST_DATA_JOB));

			// then
			verify(jesClient).getJobDetails(any());
			verify(jesClient).setJesFilters("NAME", JobStatus.ALL, "OWNER", 1024);
			verify(jesClient.getFtpClient()).listFiles("ID");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listFiles(any())).thenReturn(new JesFtpFile[0]);

			// when
			assertEquals(Optional.empty(), jesClient.getJobDetails(TEST_DATA_JOB));

			// then
			verify(jesClient).getJobDetails(any());
			verify(jesClient).setJesFilters("NAME", JobStatus.ALL, "OWNER", 1024);
			verify(jesClient.getFtpClient()).listFiles("ID");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#getServerProperties()}
	 */
	@Test
	public void testGetServerProperties() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().stat()).thenReturn(FTPReply.COMMAND_OK);
			when(jesClient.getFtpClient().getReplyStrings()).thenReturn(new String[0]);

			// when
			assertEquals(emptyMap(), jesClient.getServerProperties());

			// then
			verify(jesClient).getServerProperties();
			verify(jesClient.getFtpClient()).stat();
			verify(jesClient.getFtpClient()).getReplyStrings();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().stat()).thenReturn(FTPReply.COMMAND_OK);
			when(jesClient.getFtpClient().getReplyStrings()).thenReturn(new String[] { "abc", "211-a IS b", "def" });

			// when
			final Map<String, String> properties = new LinkedHashMap<>();
			properties.put("a", "b");
			assertEquals(properties, jesClient.getServerProperties());

			// then
			verify(jesClient).getServerProperties();
			verify(jesClient.getFtpClient()).stat();
			verify(jesClient.getFtpClient()).getReplyStrings();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().stat()).thenReturn(FTPReply.COMMAND_OK);
			when(jesClient.getFtpClient().getReplyStrings()).thenReturn(new String[] { "211-a IS b", "211-c IS d" });

			// when
			final Map<String, String> properties = new LinkedHashMap<>();
			properties.put("a", "b");
			properties.put("c", "d");
			assertEquals(properties, jesClient.getServerProperties());

			// then
			verify(jesClient).getServerProperties();
			verify(jesClient.getFtpClient()).stat();
			verify(jesClient.getFtpClient()).getReplyStrings();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().stat()).thenReturn(FTPReply.COMMAND_OK);
			when(jesClient.getFtpClient().getReplyStrings()).thenReturn(new String[] { "211-a IS b", "211-a IS d" });

			// when
			assertThrows(JesException.class, () -> jesClient.getServerProperties());

			// then
			verify(jesClient).getServerProperties();
			verify(jesClient.getFtpClient()).stat();
			verify(jesClient.getFtpClient()).getReplyStrings();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().stat()).thenReturn(FTPReply.COMMAND_NOT_IMPLEMENTED);

			// when
			assertThrows(JesException.class, () -> jesClient.getServerProperties());

			// then
			verify(jesClient).getServerProperties();
			verify(jesClient.getFtpClient()).stat();
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#list(String)}, {@link JesClient#list(String, JobStatus)},
	 * {@link JesClient#list(String, JobStatus, String)} and
	 * {@link JesClient#list(String, JobStatus, String, int)}
	 */
	@Test
	public void testList() {
		final String nameFilter = "nameFilter";
		final JobStatus status = JobStatus.OUTPUT;
		final String ownerFilter = "ownerFilter";
		final int limit = 123;
		final List<Job> jobs = asList(TEST_DATA_JOB);

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).list(any(), any());

			// when
			assertEquals(jobs, jesClient.list(nameFilter));

			// then
			verify(jesClient).list(any());
			verify(jesClient).list(nameFilter, JobStatus.ALL);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).list(any(), any(), any());

			// when
			assertEquals(jobs, jesClient.list(nameFilter, status));

			// then
			verify(jesClient).list(any(), any());
			verify(jesClient).list(nameFilter, status, "*");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).list(any(), any(), any(), anyInt());

			// when
			assertEquals(jobs, jesClient.list(nameFilter, status, ownerFilter));

			// then
			verify(jesClient).list(any(), any(), any());
			verify(jesClient).list(nameFilter, status, ownerFilter, 1024);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames()).thenReturn(new String[] { "id", "id" });
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertEquals(asList(TEST_DATA_JOB, TEST_DATA_JOB), jesClient.list(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).list(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listNames();
			verify(jesClient).throwIfLimitReached(limit, asList(TEST_DATA_JOB, TEST_DATA_JOB));
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames()).thenReturn(new String[] { "id" });
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertEquals(jobs, jesClient.list(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).list(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listNames();
			verify(jesClient).throwIfLimitReached(limit, jobs);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames()).thenReturn(new String[0]);
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertEquals(emptyList(), jesClient.list(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).list(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listNames();
			verify(jesClient).throwIfLimitReached(limit, emptyList());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames()).thenReturn(null);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("");

			// when
			assertThrows(JesException.class, () -> jesClient.list(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).list(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listNames();
			verify(jesClient.getFtpClient(), times(2)).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames()).thenReturn(null);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("550 NO JOBS FOUND FOR ...");

			// when
			assertEquals(emptyList(), jesClient.list(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).list(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listNames();
			verify(jesClient.getFtpClient(), times(2)).getReplyString();
			verify(jesClient).throwIfLimitReached(limit, emptyList());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#listFilled(String)},
	 * {@link JesClient#listFilled(String, JobStatus)},
	 * {@link JesClient#listFilled(String, JobStatus, String)} and
	 * {@link JesClient#listFilled(String, JobStatus, String, int)}
	 */
	@Test
	public void testListFilled() {
		final String nameFilter = "nameFilter";
		final JobStatus status = JobStatus.OUTPUT;
		final String ownerFilter = "ownerFilter";
		final int limit = 123;
		final List<Job> jobs = asList(TEST_DATA_JOB);

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).listFilled(any(), any());

			// when
			assertEquals(jobs, jesClient.listFilled(nameFilter));

			// then
			verify(jesClient).listFilled(any());
			verify(jesClient).listFilled(nameFilter, JobStatus.ALL);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).listFilled(any(), any(), any());

			// when
			assertEquals(jobs, jesClient.listFilled(nameFilter, status));

			// then
			verify(jesClient).listFilled(any(), any());
			verify(jesClient).listFilled(nameFilter, status, "*");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).listFilled(any(), any(), any(), anyInt());

			// when
			assertEquals(jobs, jesClient.listFilled(nameFilter, status, ownerFilter));

			// then
			verify(jesClient).listFilled(any(), any(), any());
			verify(jesClient).listFilled(nameFilter, status, ownerFilter, 1024);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listFiles())
					.thenReturn(new JesFtpFile[] { new JesFtpFile(TEST_DATA_JOB, "") });
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertEquals(jobs, jesClient.listFilled(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).listFilled(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listFiles();
			verify(jesClient).throwIfLimitReached(limit, jobs);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listFiles()).thenReturn(
					new JesFtpFile[] { new JesFtpFile(TEST_DATA_JOB, ""), new JesFtpFile(TEST_DATA_JOB, "") });
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertEquals(asList(TEST_DATA_JOB, TEST_DATA_JOB),
					jesClient.listFilled(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).listFilled(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listFiles();
			verify(jesClient).throwIfLimitReached(limit, asList(TEST_DATA_JOB, TEST_DATA_JOB));
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listFiles()).thenReturn(new JesFtpFile[0]);
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertEquals(emptyList(), jesClient.listFilled(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).listFilled(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listFiles();
			verify(jesClient).throwIfLimitReached(limit, emptyList());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#login(String, String)}
	 */
	@Test
	public void testLogin() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().login(any(), any())).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(any())).thenReturn(true);
			doNothing().when(jesClient).enterJesMode();
			doNothing().when(jesClient).setJesOwner(any());

			// when
			jesClient.login("username", "password");

			// then
			verify(jesClient).login(any(), any());
			verify(jesClient.getFtpClient()).login("username", "password");
			verify(jesClient).setJesOwner("username");
			verify(jesClient).enterJesMode();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().login(any(), any())).thenReturn(false);
			when(jesClient.getFtpClient().sendSiteCommand(any())).thenReturn(true);

			// when
			assertThrows(JesException.class, () -> jesClient.login("username", "password"));

			// then
			verify(jesClient).login(any(), any());
			verify(jesClient.getFtpClient()).login("username", "password");
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#retrieve(JobOutput)}
	 */
	@Test
	public void testRetrieve() {
		final Job job = new Job("id", "name", JobStatus.OUTPUT, "owner");
		final JobOutput jobOutput1
				= job.createOutput(1, "outputName", 10, Optional.empty(), Optional.empty(), Optional.empty());
		final JobOutput jobOutput2
				= job.createOutput(2, "outputName", 10, Optional.of("step"), Optional.empty(), Optional.empty());

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().retrieveFile(any(), any())).then(in -> {
				((OutputStream) in.getArgument(1)).write("jobOutput1".getBytes(Charset.defaultCharset()));
				return true;
			});

			// when
			assertEquals("jobOutput1", jesClient.retrieve(jobOutput1));

			// then
			verify(jesClient).retrieve(any());
			verify(jesClient.getFtpClient()).retrieveFile(eq("ID.1"), any());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().retrieveFile(any(), any())).then(in -> {
				((OutputStream) in.getArgument(1)).write("jobOutput2".getBytes(Charset.defaultCharset()));
				return true;
			});

			// when
			assertEquals("jobOutput2", jesClient.retrieve(jobOutput2));

			// then
			verify(jesClient).retrieve(any());
			verify(jesClient.getFtpClient()).retrieveFile(eq("ID.2"), any());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().retrieveFile(any(), any())).thenReturn(false);

			// when
			assertThrows(JesException.class, () -> jesClient.retrieve(jobOutput1));

			// then
			verify(jesClient).retrieve(any());
			verify(jesClient.getFtpClient()).retrieveFile(eq("ID.1"), any());
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#retrieveOutputs(Job)}
	 */
	@Test
	public void testRetrieveOutputs() {
		final Job job = new Job("id", "name", JobStatus.OUTPUT, "owner");
		final JobOutput jobOutput1
				= job.createOutput(1, "outputName", 10, Optional.empty(), Optional.empty(), Optional.empty());
		final JobOutput jobOutput2
				= job.createOutput(2, "outputName", 10, Optional.of("step"), Optional.empty(), Optional.empty());

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn("jobOutput1").when(jesClient).retrieve(jobOutput1);
			doReturn("jobOutput2").when(jesClient).retrieve(jobOutput2);

			// when
			final Map<JobOutput, String> jobOutputs = new LinkedHashMap<>();
			jobOutputs.put(jobOutput1, "jobOutput1");
			jobOutputs.put(jobOutput2, "jobOutput2");
			assertEquals(jobOutputs, jesClient.retrieveOutputs(job));

			// then
			verify(jesClient).retrieveOutputs(any());
			verify(jesClient).retrieve(jobOutput1);
			verify(jesClient).retrieve(jobOutput2);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(Optional.of(job)).when(jesClient).getJobDetails(any());
			doReturn("jobOutput1").when(jesClient).retrieve(jobOutput1);
			doReturn("jobOutput2").when(jesClient).retrieve(jobOutput2);

			// when
			final Map<JobOutput, String> jobOutputs = new LinkedHashMap<>();
			jobOutputs.put(jobOutput1, "jobOutput1");
			jobOutputs.put(jobOutput2, "jobOutput2");
			assertEquals(jobOutputs, jesClient.retrieveOutputs(TEST_DATA_JOB));

			// then
			verify(jesClient, times(2)).retrieveOutputs(any());
			verify(jesClient).getJobDetails(TEST_DATA_JOB);
			verify(jesClient).retrieve(jobOutput1);
			verify(jesClient).retrieve(jobOutput2);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(Optional.empty()).when(jesClient).getJobDetails(any());

			// when
			assertThrows(JesException.class, () -> jesClient.retrieveOutputs(TEST_DATA_JOB));

			// then
			verify(jesClient).retrieveOutputs(any());
			verify(jesClient).getJobDetails(TEST_DATA_JOB);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#setJesFilters(String, JobStatus, String, int)}
	 */
	@Test
	@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_INFERRED",
			justification = "Return value of Reflect.on(JesClient).call(\"setJesFilters\", ...) is of type void.")
	public void testSetJesFilters() {
		final String nameFilter = "nameFilter";
		final String ownerFilter = "ownerFilter";
		final JobStatus status = JobStatus.OUTPUT;
		final int limit = 123;

		final String siteCommandNameFilter = "JESJOBName=" + nameFilter;
		final String siteCommandOwnerFilter = "JESOwner=" + ownerFilter;
		final String siteCommandStatusFilter = "JESSTatus=" + status.getValue();
		final String siteCommandLimitFilter = "JESENTRYLIMIT=" + Integer.toString(limit);

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandNameFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandOwnerFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandStatusFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandLimitFilter)).thenReturn(true);

			// when
			Reflect.on(jesClient).call("setJesFilters", nameFilter, status, ownerFilter, limit);

			// then
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandNameFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandOwnerFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandStatusFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandLimitFilter);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandNameFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandOwnerFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandStatusFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandLimitFilter)).thenReturn(false);

			// when
			assertThrows(JesException.class,
					() -> Reflects
							.call(Reflect.on(jesClient), "setJesFilters", nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandNameFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandOwnerFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandStatusFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandLimitFilter);
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandNameFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandOwnerFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandStatusFilter)).thenReturn(false);

			// when
			assertThrows(JesException.class,
					() -> Reflects
							.call(Reflect.on(jesClient), "setJesFilters", nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandNameFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandOwnerFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandStatusFilter);
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandNameFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandOwnerFilter)).thenReturn(false);

			// when
			assertThrows(JesException.class,
					() -> Reflects
							.call(Reflect.on(jesClient), "setJesFilters", nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandNameFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandOwnerFilter);
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandNameFilter)).thenReturn(false);

			// when
			assertThrows(JesException.class,
					() -> Reflects
							.call(Reflect.on(jesClient), "setJesFilters", nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandNameFilter);
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#submit(String)}
	 */
	@Test
	@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
			justification = "Return value of Mockedverify(jesClient).getJesOwner() does not matter.")
	public void testSubmit() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().storeUniqueFile(any(), any())).then(invocation -> {
				try (InputStream inputStream = invocation.getArgument(1)) {
					assertEquals("jclContent", IOUtils.toString(inputStream, Charset.defaultCharset()));
				}
				return true;
			});
			when(jesClient.getFtpClient().getReplyString()).thenReturn("250-IT IS KNOWN TO JES AS id");

			// when
			assertEquals(new Job("id", "*", JobStatus.INPUT, "*"), jesClient.submit("jclContent"));

			// then
			verify(jesClient).submit(any());
			verify(jesClient.getFtpClient()).storeUniqueFile(any(), any());
			verify(jesClient.getFtpClient()).getReplyString();
			verify(jesClient).getJesOwner();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().storeUniqueFile(any(), any())).thenReturn(true);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("250-IT IS KNOWN TO JES AS id");

			// when
			assertEquals(new Job("id", "name", JobStatus.INPUT, "*"), jesClient.submit("//name\njclContent"));

			// then
			verify(jesClient).submit(any());
			verify(jesClient.getFtpClient()).storeUniqueFile(any(), any());
			verify(jesClient.getFtpClient()).getReplyString();
			verify(jesClient).getJesOwner();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().storeUniqueFile(any(), any())).thenReturn(false);

			// when
			assertThrows(JesException.class, () -> jesClient.submit("jclContent"));

			// then
			verify(jesClient).submit(any());
			verify(jesClient.getFtpClient()).storeUniqueFile(any(), any());
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().storeUniqueFile(any(), any())).thenReturn(true);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("");

			// when
			assertThrows(JesException.class, () -> jesClient.submit("jclContent"));

			// then
			verify(jesClient).submit(any());
			verify(jesClient.getFtpClient()).storeUniqueFile(any(), any());
			verify(jesClient.getFtpClient(), times(2)).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#throwIfLimitReached(int, List)}
	 */
	@Test
	public void testThrowIfLimitReached() {
		final int limit = 123;
		final List<Job> jobs = asList(TEST_DATA_JOB);

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().getReplyString()).thenReturn("");

			// when
			assertEquals(jobs, Reflect.on(jesClient).call("throwIfLimitReached", limit, jobs).get());

			// then
			verify(jesClient).throwIfLimitReached(limit, jobs);
			verify(jesClient.getFtpClient()).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().getReplyString())
					.thenReturn("250-JESENTRYLIMIT OF 456 REACHED.  ADDITIONAL ENTRIES NOT DISPLAYED");

			// when
			final JesLimitReachedException exception = assertThrows(JesLimitReachedException.class,
					() -> Reflects.call(Reflect.on(jesClient), "throwIfLimitReached", limit, jobs));

			// then
			verify(jesClient).throwIfLimitReached(limit, jobs);
			verify(jesClient.getFtpClient(), times(2)).getReplyString();
			verifyEnd(jesClient);
			assertEquals(limit, exception.getLimit());
			assertEquals(jobs, exception.getJobs());
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#waitFor(Job, Duration, Duration)},
	 * {@link JesClient#waitFor(Job, Duration, Duration, Consumer)}
	 */
	@Test
	public void testWaitFor() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(true).when(jesClient).waitFor(any(), any(), any(), any());

			// when
			jesClient.waitFor(TEST_DATA_JOB, Duration.ofMillis(123), Duration.ofMillis(456));

			// then
			verify(jesClient).waitFor(any(), any(), any());
			verify(jesClient).waitFor(any(), any(), any(), any());
			verifyEnd(jesClient);
		} catch (final InterruptedException | JesException e) {
			throw new SneakyException(e);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			final AtomicBoolean existsActive = new AtomicBoolean(true);
			doAnswer(invocation -> existsActive.getAndSet(false)).when(jesClient).exists(any(), any());
			final Job job = new Job("id", "name", JobStatus.ACTIVE, "owner");
			final Duration sleepDuration = Duration.ofMillis(123);

			// when
			final Stopwatch stopwatch = new Stopwatch();
			assertTrue(jesClient.waitFor(job, sleepDuration, Duration.ofMillis(456)));
			assertTrue(stopwatch.sinceLast().toMillis() > sleepDuration.toMillis());

			// then
			verify(jesClient).waitFor(any(), any(), any());
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient, times(2)).exists(any(), any());
			verifyEnd(jesClient);
		} catch (final InterruptedException | JesException e) {
			throw new SneakyException(e);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {

			// when
			final AtomicInteger sleepCalls = new AtomicInteger(0);
			assertTrue(jesClient.waitFor(TEST_DATA_JOB,
					Duration.ofMillis(123),
					Duration.ofMillis(456),
					duration -> sleepCalls.incrementAndGet()));
			assertEquals(0, sleepCalls.get());

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(false).when(jesClient).exists(any(), any());

			// when
			final Job job = new Job("id", "name", JobStatus.ACTIVE, "owner");
			final AtomicInteger sleepCalls = new AtomicInteger(0);
			assertTrue(jesClient.waitFor(job,
					Duration.ofMillis(123),
					Duration.ofMillis(456),
					duration -> sleepCalls.incrementAndGet()));
			assertEquals(0, sleepCalls.get());

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient).exists(job, JobStatus.ACTIVE);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			final AtomicBoolean existsActive = new AtomicBoolean(true);
			doAnswer(invocation -> existsActive.getAndSet(false)).when(jesClient).exists(any(), any());

			// when
			final Job job = new Job("id", "name", JobStatus.ACTIVE, "owner");
			final AtomicInteger sleepCalls = new AtomicInteger(0);
			assertTrue(jesClient.waitFor(job,
					Duration.ofMillis(123),
					Duration.ofMillis(456),
					duration -> sleepCalls.incrementAndGet()));
			assertEquals(1, sleepCalls.get());

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient, times(2)).exists(job, JobStatus.ACTIVE);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doAnswer(invocation -> {
				Thread.sleep(123);
				return true;
			}).when(jesClient).exists(any(), any());

			// when
			final Job job = new Job("id", "name", JobStatus.ACTIVE, "owner");
			final Consumer<Duration> noop = duration -> {
				// empty by design
			};
			assertFalse(jesClient.waitFor(job, Duration.ofMillis(123), Duration.ofMillis(456), noop));

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient, atLeastOnce()).exists(job, JobStatus.ACTIVE);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			final AtomicBoolean existsActive = new AtomicBoolean(true);
			doAnswer(invocation -> existsActive.getAndSet(false)).when(jesClient).exists(any(), any());

			// when
			final Job job = new Job("id", "name", JobStatus.ACTIVE, "owner");
			final AtomicInteger sleepCalls = new AtomicInteger(0);
			final Consumer<Duration> sleep = ThrowingConsumer.throwing(duration -> {
				sleepCalls.incrementAndGet();

				final long millis = Nullables.orElseThrow(duration).toMillis();
				Assertions.assertTrue(millis <= 123);
				Thread.sleep(millis);
			});
			assertFalse(jesClient.waitFor(job, Duration.ofMillis(456), Duration.ofMillis(123), sleep));
			assertEquals(1, sleepCalls.get());

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient).exists(job, JobStatus.ACTIVE);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			final Job job = new Job("id", "name", JobStatus.INPUT, "owner");
			final AtomicBoolean existsInput = new AtomicBoolean(true);
			doAnswer(invocation -> existsInput.getAndSet(false)).when(jesClient).exists(job, JobStatus.INPUT);
			final AtomicBoolean existsActive = new AtomicBoolean(true);
			doAnswer(invocation -> existsActive.getAndSet(false)).when(jesClient).exists(job, JobStatus.ACTIVE);

			// when
			final AtomicInteger sleepCalls = new AtomicInteger(0);
			assertTrue(jesClient.waitFor(job,
					Duration.ofMillis(123),
					Duration.ofMillis(456),
					duration -> sleepCalls.incrementAndGet()));
			assertEquals(2, sleepCalls.get());

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient, times(2)).exists(job, JobStatus.INPUT);
			verify(jesClient, times(2)).exists(job, JobStatus.ACTIVE);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * Mocked {@link JesClient}
	 */
	@NoArgsConstructor
	private static class MockedJesClient extends JesClient {
		/**
		 * Constructs a new {@link MockedJesClient} instance.
		 *
		 * <p>
		 * This factory method handles the closable warnings.
		 *
		 * @return constructed {@link MockedJesClient} instance
		 */
		@SuppressWarnings("resource")
		public static MockedJesClient newInstance() {
			return spy(new MockedJesClient());
		}

		/**
		 * Mocked FTP client
		 */
		@NonFinal
		@Nullable
		FTPClient ftpClient;

		/** {@inheritDoc} */
		@Override
		public FTPClient getFtpClient() {
			if (ftpClient == null) {
				ftpClient = mock(FTPClient.class);
			}
			return Nullables.orElseThrow(ftpClient);
		}
	}
}
