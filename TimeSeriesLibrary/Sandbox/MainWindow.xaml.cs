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

using System.IO;

using System.Security.Cryptography;

using TimeSeriesLibrary;

namespace Sandbox
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        const int nVals = 30000, nIter = 50, nTrc = 100;
        DateTime StartDate = new DateTime(1900, 1, 1, 23, 59, 0);
        DateTime TimerStart;

        public MainWindow()
        {
            InitializeComponent();
        }

        private unsafe int WritingTest()
        {
            //String paramTableName = "OutputTimeSeries";
            //String traceTableName = "OutputTimeSeriesTraces";
            //String extraParamNames = 
            //        "RunGUID, TimeSeriesType, VariableType, Unit_Id, VariableName, RunElementGUID";
            //String extraParamValues =
            //        "'00000000-0000-0000-0000-000000000000', 2, 'Dummy', 2, 'DELETEME', '00000000-0000-0000-0000-000000000000'";

            //int connectionNumber = 1;
            //TSLibrary tsLib = new TSLibrary();
            //connectionNumber = tsLib.OpenConnection(
            //            "Data Source=.; Database=NYC-SpeedTest; Trusted_Connection=yes;");
            
            List<Double> valList = new List<Double>();
            DateTime date = StartDate;

            int x = 0;
            for (int i = 0; i < nVals; i++)
            {
                x = Math.Abs(x - 1);
                valList.Add(i*Math.PI);
                date = date.AddDays(1);
            }
            var valArray = valList.ToArray();

            byte[] blob = null;
            for (int j = 0; j < nIter; j++)
            {
                //var ts = new TS(tsLib.ConnxObject.TSConnectionsCollection[connectionNumber],
                //                tsLib.ConnxObject, paramTableName, traceTableName);

                //int id = ts.WriteParametersRegular(true, null,
                //                (short)TSDateCalculator.TimeStepUnitCode.Day, 1, nVals, StartDate,
                //                extraParamNames, extraParamValues);
                ////int id = 800291;

                for (int i = 0; i < nTrc; i++)
                {
                    //ts.WriteTraceRegular(id, true, null, i + 1, valArray);


                    ITimeSeriesTrace traceObject = new TSTrace
                    {
                        TraceNumber = i+1,
                        TimeStepCount = nVals,
                        EndDate = date
                    };

                    blob = TSBlobCoder.ConvertArrayToBlobRegular(valArray, 2, traceObject);
                }

            }
            return blob.Length;
        }
        
        private void MainWindowClosed(object sender, EventArgs e)
        {
            //ctsLib.CloseConnection(connNumber);
            //tsLib.CloseConnection(connNumber);
        }
        

        private void GoButtonClick(object sender, RoutedEventArgs e)
        {
            TimeLabelBlob.Content = "WAITING";
            TimerStart = DateTime.Now;

            var worker = new System.ComponentModel.BackgroundWorker();
            worker.DoWork += worker_DoWork;
            worker.RunWorkerCompleted += worker_RunWorkerCompleted;
            worker.RunWorkerAsync();
        }

        void worker_RunWorkerCompleted(object sender, System.ComponentModel.RunWorkerCompletedEventArgs e)
        {
            TimeSpan timerDiff = DateTime.Now - TimerStart;
            TimeLabelBlob.Content = e.Result + "\n" 
                        + String.Format("Duration: {0:hh\\:mm\\:ss\\.f}", timerDiff);
        }

        void worker_DoWork(object sender, System.ComponentModel.DoWorkEventArgs e)
        {
            int size = WritingTest();
            e.Result = "SIZE: " + size / 1000 + " K";
        }



        /// <summary>
        /// This method converts the given string to a byte array, which is equivalent
        /// to a char[] array used in native C++ code.
        /// </summary>
        private unsafe static sbyte[] GetSbyte(String s)
        {
            byte[] b = System.Text.Encoding.ASCII.GetBytes(s);

            sbyte[] sb = new sbyte[b.Length];

            for (int i = 0; i < b.Length; i++)
                sb[i] = (sbyte)b[i];

            return sb;
        }
        
        
    }
}











