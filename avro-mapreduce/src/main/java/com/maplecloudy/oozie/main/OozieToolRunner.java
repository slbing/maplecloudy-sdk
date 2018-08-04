package com.maplecloudy.oozie.main;

import java.io.PrintStream;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * A utility to help run {@link Tool}s.
 * 
 * <p>
 * <code>ToolRunner</code> can be used to run classes implementing
 * <code>Tool</code> interface. It works in conjunction with
 * {@link GenericOptionsParser} to parse the <a href="{@docRoot} to parse the
 * <a href="
 * {@docRoot} /org/apache/hadoop/util/GenericOptionsParser.html#GenericOptions">
 * generic hadoop command line arguments</a> and modifies the
 * <code>Configuration</code> of the <code>Tool</code>. The application-specific
 * options are passed along without being modified.
 * </p>
 * 
 * @see Tool
 * @see GenericOptionsParser
 */
public class OozieToolRunner {

	/**
	 * Runs the given <code>Tool</code> by {@link Tool#run(String[])}, after
	 * parsing with the given generic arguments. Uses the given
	 * <code>Configuration</code>, or builds one if null.
	 * 
	 * Sets the <code>Tool</code>'s configuration with the possibly modified
	 * version of the <code>conf</code>.
	 * 
	 * @param conf
	 *            <code>Configuration</code> for the <code>Tool</code>.
	 * @param tool
	 *            <code>Tool</code> to run.
	 * @param args
	 *            command-line arguments to the tool.
	 * @return exit code of the {@link Tool#run(String[])} method.
	 */
	public static int run(Configuration conf, OozieMain tool, String[] args) throws Exception {
		System.getProperties().store(System.out, "corrent properties:");

		String user = System.getProperties().getProperty("user.name");
		if (conf == null) {
			conf = new Configuration();
		}
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		tool.setConf(conf);
		tool.loadOozieConf();
		// get the args w/o generic hadoop args
		String[] toolArgs = parser.getRemainingArgs();

		if (user == null || "".equals(user.trim())) {
			return tool.run(toolArgs);
		} else {
			UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
			int ret = ugi.doAs(new PrivilegedAction<Integer>() {
				@Override
				public Integer run() {
					try {
						return tool.run(toolArgs);
					} catch (Exception e) {
						e.printStackTrace();
					}
					return -1;
				}

			});
			return ret;
		}

	}

	/**
	 * Runs the <code>Tool</code> with its <code>Configuration</code>.
	 * 
	 * Equivalent to <code>run(tool.getConf(), tool, args)</code>.
	 * 
	 * @param tool
	 *            <code>Tool</code> to run.
	 * @param args
	 *            command-line arguments to the tool.
	 * @return exit code of the {@link Tool#run(String[])} method.
	 */
	public static int run(OozieMain tool, String[] args) throws Exception {
		return run(tool.getConf(), tool, args);
	}

	/**
	 * Prints generic command-line argurments and usage information.
	 * 
	 * @param out
	 *            stream to write usage information to.
	 */
	public static void printGenericCommandUsage(PrintStream out) {
		GenericOptionsParser.printGenericCommandUsage(out);
	}

}
