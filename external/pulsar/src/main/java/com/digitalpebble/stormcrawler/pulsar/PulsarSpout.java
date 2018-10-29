/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
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

package com.digitalpebble.stormcrawler.pulsar;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.storm.MessageToValuesMapper;
import org.apache.pulsar.storm.PulsarSpoutConfiguration;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/** Wrapper for PulsarSpout **/

public class PulsarSpout extends BaseRichSpout implements MessageToValuesMapper {

    private BaseRichSpout pulsarSpout;

    @Override
    public void open(Map config, TopologyContext context,
            SpoutOutputCollector collector) {
        String service = ConfUtils.getString(config, "pulsar.spout.serviceUrl",
                "pulsar://localhost:6650");
        String topic = ConfUtils.getString(config, "pulsar.spout.topic");
        String subscription = ConfUtils.getString(config,
                "pulsar.spout.subscription");
        PulsarSpoutConfiguration spoutConf = new PulsarSpoutConfiguration();
        spoutConf.setTopic(topic);
        spoutConf.setSubscriptionName(subscription);
        spoutConf.setServiceUrl(service);
        spoutConf.setMessageToValuesMapper(this);

        ClientBuilder builder = PulsarClient.builder();
        pulsarSpout = new org.apache.pulsar.storm.PulsarSpout(spoutConf,
                builder);
        pulsarSpout.open(config, context, collector);
    }

    @Override
    public void nextTuple() {
        pulsarSpout.nextTuple();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata", "status"));
    }

    @Override
    public Values toValues(Message<byte[]> msg) {
        // URL - STATUS - METADATA
        Metadata metadata = new Metadata();
        String[] tokens = new String(msg.getData(), StandardCharsets.UTF_8)
                .split("\t");
        String url = tokens[0];
        String status = tokens[1];

        for (int i = 2; i < tokens.length; i++) {
            String token = tokens[i];
            // split into key & value
            int firstequals = token.indexOf("=");
            String value = null;
            String key = token;
            if (firstequals != -1) {
                key = token.substring(0, firstequals);
                value = token.substring(firstequals + 1);
            }
            metadata.addValue(key, value);
        }

        return new Values(url, metadata, Status.valueOf(status));
    }

}
