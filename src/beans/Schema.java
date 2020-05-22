package beans;

import java.util.ArrayList;

public class Schema {

    public String sourceDatabase;
    public ArrayList<SingleTable> singleTables;


    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public ArrayList<SingleTable> getSingleTables() {
        return singleTables;
    }
}
