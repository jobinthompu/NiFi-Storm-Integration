package NiFi;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.storm.NiFiDataPacket;
import org.apache.nifi.storm.NiFiSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class NiFiStormStreaming {

private static Connection con;

public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {	}
public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, SQLException, UnknownHostException {

final String zk_hostname = InetAddress.getLocalHost().getHostName();
final String nifi_hostname = InetAddress.getLocalHost().getHostName();
final int nifi_port = 9090;
final String nifi_port_name = "OUT";
SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
.url("http://"+nifi_hostname+":"+nifi_port+"/nifi")
.portName(nifi_port_name)
.buildConfig();
System.setProperty("hadoop.home.dir", "/");    
//Build a topology starting with a NiFiSpout
TopologyBuilder builder = new TopologyBuilder();  
builder.setSpout("nifi", new NiFiSpout(clientConfig));   
//Add a bolt that prints the attributes and content
builder.setBolt("Phoenix-Insert", new BaseBasicBolt() {
private static final long serialVersionUID = 1L;

public void execute(Tuple tuple, BasicOutputCollector collector)

	{
				NiFiDataPacket dp = (NiFiDataPacket) tuple.getValueByField("nifiDataPacket");
				//Print Attributes and Contents, can find this in Storm Logs
				System.err.println("###################################################################################################");
				System.out.println("UUID: " + dp.getAttributes().get("uuid"));
				System.out.println("BULLETIN_LEVEL: " + dp.getAttributes().get("BULLETIN_LEVEL"));
				System.out.println("EVENT_DATE: " + dp.getAttributes().get("EVENT_DATE"));
				System.out.println("EVENT_TYPE: " + dp.getAttributes().get("EVENT_TYPE"));
				System.out.println("Content: " + new String(dp.getContent()));
				System.err.println("###################################################################################################");
				try {
				//Extracting NiFi FLowFIle Attributes
				String UUID = (String) dp.getAttributes().get("uuid");
				String BULLETIN_LEVEL = (String) dp.getAttributes().get("BULLETIN_LEVEL");
				
				
				String NIFI_DATE = (String) dp.getAttributes().get("EVENT_DATE");
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date NIFI_DATES = (Date) formatter.parse(NIFI_DATE); 
				java.sql.Timestamp EVENT_DATE = new java.sql.Timestamp(NIFI_DATES.getTime());

				
				String EVENT_TYPE = (String) dp.getAttributes().get("EVENT_TYPE");
				//Extracting NiFi FLowFIle Content
				String CONTENT = new String(dp.getContent());
				//Print Upsert Query, can find this in Storm Logs
				System.err.println("upsert into NIFI_LOG values ("+UUID+","+BULLETIN_LEVEL+","+EVENT_DATE+","+EVENT_TYPE+","+CONTENT+")");
				Class.forName("org.apache.phoenix.jdbc.PhoenixDriver").newInstance();
				con = DriverManager.getConnection("jdbc:phoenix:"+zk_hostname+":2181:/hbase-unsecure");
			    con.setAutoCommit(true);
				Statement stmt = con.createStatement();
				stmt.executeUpdate("upsert into NIFI_LOG values ('"+UUID+"','"+EVENT_DATE+"','"+BULLETIN_LEVEL+"','"+EVENT_TYPE+"','"+CONTENT+"')");
					}
				catch (SQLException e) {
					e.printStackTrace();
					} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}).shuffleGrouping("nifi");
Config conf = new Config();
//Submit the topology running in Cluster mode
StormSubmitter.submitTopology("NiFi-Storm-Phoenix", conf, builder.createTopology());
//Comment above line and uncomment below two lines to run in local mode
//LocalCluster cluster = new LocalCluster();
//cluster.submitTopology("test", conf, builder.createTopology());
}

}
