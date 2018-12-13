# JES Client
Handling IBM z/OS JES spools using Java technologies based on the IBM z/OS FTP server.

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
		Job submittedJob = jesClient.submit(jclContent);
		
		// Wait for submitted job to be finished
		Optional<Job> finishedJob = jesClient.waitFor(submittedJob);
		
		// Handle the case, a finished job cannot be found inside JES spool any longer
		if (!finishedJob.isPresent()) {
			...
		} else {
		
			// Gather finished jobs outputs
			List<JobOutput> jobOutput = jesClient.get(finishedJob.get());
		
			// Delete job from JES spool
			jesClient.delete(finishedJob.get());
		}
		
	// Logout and disconnect using try-with-resource (close method)
	}