# JES Client
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Flars-sh%2Fjes-client.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Flars-sh%2Fjes-client?ref=badge_shield)

Handling IBM z/OS JES spools using Java technologies based on the IBM z/OS FTP server.

[Changelog](CHANGELOG.md)  |  [JavaDoc](https://lars-sh.github.io/jes-client/apidocs)  |  [Generated Reports](https://lars-sh.github.io/jes-client/project-reports.html)

## Getting started
Here's a Maven dependency example:

	<dependency>
		<groupId>de.lars-sh</groupId>
		<artifactId>jes-client</artifactId>
		<version><!-- TODO --></version>
	</dependency>

To learn more about the JES Client check out its JavaDoc.

### Using JES Client
The following shows the JesClient used inside a try-with-resource statement. The constructor descriptions describe further details.

	// Connect and login via simplified constructor
	try (JesClient jesClient = new JesClient(hostname, port, username, password)) {
	
		// Submit JCL
		Job job = jesClient.submit(jclContent);
	
		// Wait for job to be finished
		if (!jesClient.waitFor(job)) {
			// Handle the case, a finished job cannot be found inside JES spool any longer
			throw ...;
		}
	
		// Gather job status details
		Job detailedJob = jesClient.getJobDetails(job);
	
		// Gather finished jobs outputs
		List&lt;JobOutput&gt; jobOutput = jesClient.get(job);
	
		// Delete job from JES spool
		jesClient.delete(job);
	
	// Logout and disconnect using try-with-resource (close method)
	}


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Flars-sh%2Fjes-client.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Flars-sh%2Fjes-client?ref=badge_large)