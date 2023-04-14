package Jpos.Switch.iso;

import org.jpos.core.Configurable;
import org.jpos.core.Configuration;
import org.jpos.core.ConfigurationException;
import org.jpos.iso.*;
import org.jpos.q2.iso.QMUX;
import org.jpos.util.NameRegistrar;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SwitchRequestListener implements ISORequestListener, Configurable {
    private Map<String, String> routeTable;

    @Override
    public void setConfiguration(Configuration configuration) throws ConfigurationException {
        routeTable = new HashMap<>();
        routeTable.put("B001", "server_1");
        routeTable.put("B002", "server_2");
    }

    @Override
    public boolean process(ISOSource isoSource, ISOMsg isoMsg) {
        try {
            if (isoMsg.getMTI().equals("0200")) {
                Thread t = new Thread(new Processor(isoSource, isoMsg));
                t.start();
                return true;
            }

        } catch (ISOException e) {
            throw new RuntimeException(e);
        }

        return false;
    }

    class Processor implements Runnable {
        private ISOSource isoSource;
        private ISOMsg isoMsg;

        public Processor(ISOSource isoSource, ISOMsg isoMsg) {
            this.isoSource = isoSource;
            this.isoMsg = isoMsg;
        }

        @Override
        public void run() {
            try {
                String fiid = isoMsg.getString(120);
                String serverName = routeTable.get(fiid);

                MUX mux = QMUX.getMUX(serverName + "-mux");
                ISOMsg respMsg = mux.request(isoMsg, 30000);

                if (respMsg != null) {
                    isoSource.send(respMsg);
                }

            } catch (NameRegistrar.NotFoundException | ISOException | IOException e) {
                throw new RuntimeException(e);
            }
        }


    }


}
