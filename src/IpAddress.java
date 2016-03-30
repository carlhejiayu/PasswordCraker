/**
 * Created by herbert on 2016-03-26.
 */
public class IpAddress {
    String Ip;
    int port;

    public IpAddress(String ip, int port) {
        Ip = ip;
        this.port = port;
    }

    static public IpAddress parseAddressString(String address){
        //string in format of xxxxxx:####
        String[] s = address.split(":");
        String ip = s[0];

        int port = Integer.parseInt(s[1]);
        return new IpAddress(ip, port);
    }
}
