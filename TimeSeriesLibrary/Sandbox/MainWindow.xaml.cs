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
        const int nVals = 30000;

        int connNumber;
        TSLibrary tsLib = new TSLibrary();
        Guid testId = new Guid();

        public unsafe MainWindow()
        {
            InitializeComponent();

            connNumber = tsLib.OpenConnection(
                "Data Source=.; Database=OasisOutput; Trusted_Connection=yes;");

            WriteBlobPacked();
        }
        private void GoButtonClick(object sender, RoutedEventArgs e)
        {
            //ImportTest();
            ReadTest();
            //WriteTest();
        }

        void ImportTest()
        {
            int i;
            List<TSImport> l = new List<TSImport>();

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < 6; i++)
            {
                tsLib.XmlImportWithList(connNumber, "FileStrm2",
                            "D:\\OASIS\\_Build\\TimeSeriesLibrary\\TimeSeriesLibrary\\Sandbox\\test4.xml", l);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("Imported --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }

        void WriteBlobPacked()
        {
            int i;

            double[] valArray = new double[nVals];
            DateTime startDate = new DateTime(1928, 1, 1, 23, 59, 0);

            for (i = 0; i < nVals; i++)
                valArray[i] = i * 3;
            testId = tsLib.WriteValues(connNumber, "FileStrm2",
                       3, 1, nVals, valArray, startDate);
        }

        void ReadTest()
        {
            int ret, i;

            double[] valArray = new double[nVals];
            DateTime startDate = new DateTime(1928, 1, 1, 23, 59, 0);

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < 1200; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                ret = tsLib.ReadValues(connNumber, "FileStrm2",
                                testId, nVals, valArray, startDate);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBBED --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }


        void WriteTest()
        {
            int i;

            double[] valArray = new double[nVals];
            DateTime startDate = new DateTime(1928, 1, 1, 23, 59, 0);

            for (i = 0; i < nVals; i++)
                valArray[i] = i * 3;

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < 6600; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                tsLib.WriteValues(connNumber, "FileStrm2",
                           3, 1, nVals, valArray, startDate);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBWRI --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }


    }
}
