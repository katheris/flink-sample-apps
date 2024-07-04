/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.FileUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Main class for executing SQL scripts. */
public class SqlRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SqlRunner.class);

    private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
    private static final String LINE_DELIMITER = "\n";

    private static final String COMMENT_PATTERN = "(--.*)|(((\\/\\*)+?[\\w\\W]+?(\\*\\/)+))";

    private static final Pattern SET_STATEMENT_PATTERN =
            Pattern.compile("SET\\s+'(\\S+)'\\s+=\\s+'(.*)';", Pattern.CASE_INSENSITIVE);

    private static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERTIFICATE = "-----END CERTIFICATE-----";
    private static final String ESCAPED_BEGIN_CERTIFICATE = "======BEGIN CERTIFICATE=====";
    private static final String ESCAPED_END_CERTIFICATE = "=====END CERTIFICATE=====";

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Exception("Exactly 2 arguments are expected.");
        }

        String script = FileUtils.readFileUtf8(new File(args[0]));
        String bootstrapServers = args[1];

        var statements = parseStatements(script);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        var tableEnv = TableEnvironment.create(settings);
        System.out.println("TableEnv config: " + tableEnv.getConfig().toMap());

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        Admin admin = Admin.create(props);
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        var outputTopic = new NewTopic("flink.recommended.products", 1, (short) 1).configs(topicConfigs);
        admin.createTopics(Collections.singleton(outputTopic));

        System.out.println("Starting Click Stream Data Generator...");
        Thread csThread = new Thread(new ClickStreamDataGenerator(bootstrapServers));
        csThread.start();

        System.out.println("Starting Sales Data Generator");
        Thread kafkaThread = new Thread(new SalesDataGenerator(bootstrapServers));
        kafkaThread.start();

        for (String statement : statements) {
            Matcher setMatcher = SET_STATEMENT_PATTERN.matcher(statement.trim());

            if (setMatcher.matches()) {
                // Handle SET statements
                String key = setMatcher.group(1);
                String value = setMatcher.group(2);
                tableEnv.getConfig().getConfiguration().setString(key, value);
            } else {
                LOG.info("Executing:\n{}", statement);
                tableEnv.executeSql(statement);
            }
        }
    }

    public static List<String> parseStatements(String script) {
        var formatted =
                formatSqlFile(script)
                        .replaceAll(BEGIN_CERTIFICATE, ESCAPED_BEGIN_CERTIFICATE)
                        .replaceAll(END_CERTIFICATE, ESCAPED_END_CERTIFICATE)
                        .replaceAll(COMMENT_PATTERN, "")
                        .replaceAll(ESCAPED_BEGIN_CERTIFICATE, BEGIN_CERTIFICATE)
                        .replaceAll(ESCAPED_END_CERTIFICATE, END_CERTIFICATE);

        var statements = new ArrayList<String>();

        StringBuilder current = null;
        boolean statementSet = false;
        for (String line : formatted.split("\n")) {
            var trimmed = line.trim();
            if (trimmed.isBlank()) {
                continue;
            }
            if (current == null) {
                current = new StringBuilder();
            }
            if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
                statementSet = true;
            }
            current.append(trimmed);
            current.append("\n");
            if (trimmed.endsWith(STATEMENT_DELIMITER)) {
                if (!statementSet || trimmed.equals("END;")) {
                    statements.add(current.toString());
                    current = null;
                    statementSet = false;
                }
            }
        }
        return statements;
    }

    public static String formatSqlFile(String content) {
        String trimmed = content.trim();
        StringBuilder formatted = new StringBuilder();
        formatted.append(trimmed);
        if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
            formatted.append(STATEMENT_DELIMITER);
        }
        formatted.append(LINE_DELIMITER);
        return formatted.toString();
    }
}