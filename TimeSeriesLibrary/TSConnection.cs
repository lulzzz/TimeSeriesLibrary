using System;
using System.Collections.Generic;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace TimeSeriesLibrary
{
    public class TSConnection
    {
        Dictionary<int, SqlConnection> tSConnectionsCollection;
        public Dictionary<int, SqlConnection> TSConnectionsCollection
        {
            get
            {
                if (tSConnectionsCollection == null)
                {
                    tSConnectionsCollection = new Dictionary<int, SqlConnection>();
                }
                return tSConnectionsCollection;
            }
        }

        public int OpenConnection(String connxString)
        {
            SqlConnection connx;
            
            int key = TSConnectionsCollection.Count + 1;

            try
            {
                connx = new SqlConnection(connxString);
            }
            catch
            {
                return 0;
            }
            tSConnectionsCollection.Add(key, connx);
            return key;
        }

        public void CloseConnection(int key)
        {
            try
            {
                SqlConnection connx = TSConnectionsCollection[key];
                connx.Close();
            }
            catch
            {
                return;
            }
        }

    }
}
