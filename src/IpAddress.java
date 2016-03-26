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
        int len = address.length();
        StringBuilder stringBuilder = new StringBuilder();
        String ip = null;
        boolean isIp = true;
        for(int i = 0; i < len; i++){
            if(isIp) {
                if (address.charAt(i) != ':') {
                    stringBuilder.append(address.charAt(i));
                } else{
                    isIp = false;
                    ip = stringBuilder.toString();
                    stringBuilder = new StringBuilder();
                    System.out.println("ip is " + ip);
                }
            }
            else {
                stringBuilder.append(address.charAt(i));
            }
        }
        int port = Integer.parseInt(stringBuilder.toString());
        return new IpAddress(ip, port);
    }
}
