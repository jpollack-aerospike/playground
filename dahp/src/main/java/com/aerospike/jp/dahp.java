package com.aerospike.jp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import com.aerospike.client.AerospikeClient;
import com.aerospike.documentapi.AerospikeDocumentClient;
import com.aerospike.documentapi.util.JsonConverters;
import com.fasterxml.jackson.databind.JsonNode;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;

public class dahp {
	
    public static void main(String[] args) throws Exception {

        Options options = new Options();
	options.addOption (null, "help", false, "This message");
        options.addOption (null, "ashost", true, "Server hostname (default: 127.0.0.1)");
        options.addOption (null, "asport", true, "Server port (default: 3000)");
	options.addOption (null, "bin", true, "Bin name (default: map)");
	options.addOption (null, "ns", true, "Namespace (default: test)");
	options.addOption (null, "set", true, "Set name (default: s0)");
	options.addOption (null, "keyStart", true, "Key start (default: 0)");
	options.addOption (null, "keyStop", true, "Key stop (default: 1000)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cl = parser.parse(options, args, false);

        String ashost = cl.getOptionValue("ashost", "127.0.0.1");
        int asport = Integer.parseInt (cl.getOptionValue("asport", "3000"));
        String namespace = cl.getOptionValue("ns", "test");
        String set = cl.getOptionValue("set", "s0");
        String bin = cl.getOptionValue("bin", "map");
	long keyStart = Long.parseLong (cl.getOptionValue ("keyStart", "0"));
	long keyStop = Long.parseLong (cl.getOptionValue ("keyStop", "1000"));
        
	String[] queries = cl.getArgs ();

        if (cl.hasOption("help") || (queries.length == 0)) {
	    HelpFormatter formatter = new HelpFormatter();
	    StringWriter sw = new StringWriter();
	    PrintWriter pw = new PrintWriter(sw);
	    String syntax = "dahp [<options>] JSONPATH...";
	    formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
	    System.out.println (sw.toString());
	    return;
	}
	
	IAerospikeClient client = new AerospikeClient(new ClientPolicy(), ashost, asport);
	AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
	String query = queries[0];
	long t0 = System.currentTimeMillis ();
	for (long ki = keyStart; ki <= keyStop; ki++) {
	    Key key0 = new Key (namespace, set, ki);
	    Object obj = documentClient.get (key0, bin, query);
	    // System.out.println(JsonConverters.writeValueAsString (obj));
	}
	long t1 = System.currentTimeMillis ();
	long tdur = t1 - t0;
	long nrec = (keyStop - keyStart) + 1;
	double rat = Double.valueOf (tdur) / Double.valueOf (nrec);
	System.out.println ("queried " + nrec + " records in " + tdur + " ms (" + rat + " ms/record)");
	client.close();
    }

}
