package beans;

public class ConnArgs {
    public String userId;
    public String pwd;
    public String address;
    public String port;
    public String database;

    public String getUserId() {
        return userId;
    }

    public String getPwd() {
        return pwd;
    }

    public String getAddress() {
        return address;
    }

    public String getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }
    public String getConUrl(){
        return address+":"+port+"/"+database;
    }
}
