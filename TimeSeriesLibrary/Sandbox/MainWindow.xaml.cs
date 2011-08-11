using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

using System.Data;
using System.Data.SqlClient;

using TimeSeriesLibrary;

namespace Sandbox
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        const int nVals = 30000; //30000;

        int connNumber;
        TSMax tsLib = new TSMax();
        TSFileS tsLibFS = new TSFileS();

        public unsafe MainWindow()
        {
            InitializeComponent();

            connNumber = tsLib.ConnxObject.OpenConnection(
                "Data Source=.; Database=OasisOutput; Trusted_Connection=yes;");
            connNumber = tsLibFS.ConnxObject.OpenConnection(
                "Data Source=.; Database=OasisOutput; Trusted_Connection=yes;");

            //WriteNoBlob();
            //WriteBlobPacked();
            //WriteBlobPackedFS();
        }
        private void GoButtonClick(object sender, RoutedEventArgs e)
        {
            //ReadBlobPackedFS();
            //ReadBlobPacked();
            //ReadNoBlob();
            WriteTest();
        }


        void WriteBlobPackedFS()
        {
            int ret, i;

            double[] valArray = new double[nVals];
            DateTime startDate = new DateTime(1928, 1, 1, 23, 59, 0);

            for (i = 0; i < nVals; i++)
                valArray[i] = i * 3;
            ret = tsLibFS.WriteValues(connNumber, "FileStrm2",
                       3, 1, nVals, valArray, startDate);
        }

        void ReadBlobPackedFS()
        {
            int ret, i;

            double[] valArray = new double[nVals];
            DateTime startDate = new DateTime(1928, 1, 1, 23, 59, 0);

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < 1200; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                ret = tsLibFS.ReadValues(connNumber, "FileStrm2",
                                12029, nVals, valArray, startDate);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBBED --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }

        void WriteBlobPacked()
        {
            int ret, i;

            double[] valArray = new double[nVals];
            DateTime startDate = new DateTime(1928, 1, 1, 23, 59, 0);

            for (i = 0; i < nVals; i++)
                valArray[i] = i * 3;
            ret = tsLib.WriteValues(connNumber, "BinaryMax",
                       3, 1, nVals, valArray, startDate);
        }

        void ReadBlobPacked()
        {
            int ret, i;

            double[] valArray = new double[nVals];
            DateTime startDate = new DateTime(1928, 1, 1, 23, 59, 0);

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < 1200; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                ret = tsLib.ReadValues(connNumber, "BinaryMax",
                                11005, nVals, valArray, startDate);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBBED --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }

        void WriteNoBlob()
        {
            int i, tsId = 1;

            double[] valArray = new double[nVals];
            DateTime currDate = new DateTime(1928, 1, 1, 23, 59, 0);

            for (i = 0; i < nVals; i++)
                valArray[i] = i * 3;

            String comm = String.Format("select * from Flow_OutputTimeSeriesValues where TimeSeries_Id={0} ", tsId);
            // Send SQL resultset to DataTable dTable
            SqlDataAdapter adp = new SqlDataAdapter(comm, tsLib.ConnxObject.TSConnectionsCollection[connNumber]);
            SqlCommandBuilder bld = new SqlCommandBuilder(adp);
            DataTable dTable = new DataTable();
            adp.Fill(dTable);

            for (i = 0; i < nVals; i++)
            {
                DataRow currentRow = dTable.NewRow();

                currentRow["TimeSeries_Id"] = tsId;
                currentRow["Date"] = currDate;
                currentRow["Value"] = valArray[i];
                dTable.Rows.Add(currentRow);

                currDate = currDate.AddDays(1);
            }
            adp.Update(dTable);

        }

        void ReadNoBlob()
        {
            int i, t, tsId = 1;

            double[] valArray = new double[nVals];
            DateTime startDate = new DateTime(1928, 1, 1, 23, 59, 0);
            DateTime endDate = startDate.AddDays(nVals);

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < 120; i++)
            {
                String comm = String.Format("select Date, Value from Flow_OutputTimeSeriesValues where TimeSeries_Id={0} " +
                            "and Date between '{1:yyyyMMdd HH:mm}' and '{2:yyyyMMdd HH:mm}' ",
                            tsId, startDate, endDate);
                // Send SQL resultset to DataTable dTable
                SqlDataAdapter adp = new SqlDataAdapter(comm, tsLib.ConnxObject.TSConnectionsCollection[connNumber]);
                DataTable dTable = new DataTable();
                adp.Fill(dTable);

                for (t = 0; t < nVals; t++)
                {
                    DataRow currentRow = dTable.Rows[t];

                    DateTime currDate = (DateTime)currentRow["Date"];
                    valArray[i] = (double)currentRow["Value"];
                }
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelNoBlob.Content = String.Format("NO BLOB --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);

        }

        void WriteTest()
        {
            int ret, i;

            double[] valArray = new double[nVals];
            DateTime startDate = new DateTime(1928, 1, 1, 23, 59, 0);

            for (i = 0; i < nVals; i++)
                valArray[i] = i * 3;

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < 1200; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                ret = tsLibFS.WriteValues(connNumber, "FileStrm2",
                           3, 1, nVals, valArray, startDate);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBWRI --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }


    }
}
