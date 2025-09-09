package dm.kaist.io;

import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;

public class SerializableConfiguration extends Configuration implements Serializable {

    public SerializableConfiguration() {
        super();
        // TODO Auto-generated constructor stub
    }

    public SerializableConfiguration(Configuration other) {
        super(other);
        // TODO Auto-generated constructor stub
    }

}
