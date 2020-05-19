package beans;

public class ConnArgs {
    public String user_id;
    public String pwd;
    public String address;
    public String port;
    public String database;

    public String getUser_id() {
        return user_id;
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
