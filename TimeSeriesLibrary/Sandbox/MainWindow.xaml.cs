﻿using System;
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
        const int nVals = 30000, nIter = 1000;

        int connNumber;
        TSLibrary tsLib = new TSLibrary();
        Guid testId1 = new Guid();
        Guid testId2 = new Guid();
        DateTime StartDate = new DateTime(1928, 1, 1, 23, 59, 0);

        public MainWindow()
        {
            InitializeComponent();

            connNumber = tsLib.OpenConnection(
                "Data Source=.; Database=OasisOutput; Trusted_Connection=yes;");

            WriteOneSeriesArray();
            //WriteOneSeriesList();
            //ReadOneSeriesArray();
        }
        private void MainWindowClosed(object sender, EventArgs e)
        {
            tsLib.CloseConnection(connNumber);
        }
        

        private void GoButtonClick(object sender, RoutedEventArgs e)
        {
            //ImportTest();
            ReadArrayTest();
            //ReadListTest();
            //WriteArrayTest();
            //WriteListTest();
            //DeleteTest();

        }

        void DeleteTest()
        {
            bool ret = tsLib.DeleteMatchingSeries(connNumber, "FileStrm2", "Id > 102476");
            if (ret == false)
            {
            }
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

        void WriteOneSeriesArray()
        {
            int i;

            double[] valArray = new double[nVals];

            for (i = 0; i < nVals; i++)
                valArray[i] = i * 3;
            testId1 = tsLib.WriteValuesUnsafe(connNumber, "FileStrm2",
                       3, 1, nVals, valArray, StartDate);
        }
        void WriteOneSeriesList()
        {
            int i;

            List<double> valList = new List<double>();

            for (i = 0; i < nVals; i++)
                valList.Add(i * 10);
            testId2 = tsLib.WriteValues(connNumber, "FileStrm2",
                       3, 1, nVals, valList, StartDate);
        }
        void ReadOneSeriesArray()
        {
            int ret;

            double[] valArray = new double[nVals];

            ret = tsLib.ReadValuesUnsafe(connNumber, "FileStrm2",
                            testId1, nVals, valArray, StartDate);

            ret = tsLib.ReadValuesUnsafe(connNumber, "FileStrm2",
                            testId2, nVals, valArray, StartDate);

            ret = 3;
        }

        void ReadArrayTest()
        {
            int ret, i;

            double[] valArray = new double[nVals];

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < nIter; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                ret = tsLib.ReadValuesUnsafe(connNumber, "FileStrm2",
                                testId1, nVals, valArray, StartDate);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBBED --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }
        void ReadListTest()
        {
            int ret, i;

            List<double> valList = new List<double>();

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < nIter; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                ret = tsLib.ReadValues(connNumber, "FileStrm2",
                                testId1, nVals, ref valList, StartDate);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBBED --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }


        void WriteArrayTest()
        {
            int i;

            double[] valArray = new double[nVals];

            for (i = 0; i < nVals; i++)
                valArray[i] = i * 25;

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < nIter; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                tsLib.WriteValuesUnsafe(connNumber, "FileStrm2",
                           3, 1, nVals, valArray, StartDate);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBWRI --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }
        void WriteListTest()
        {
            int i;

            List<double> valList = new List<double>();

            for (i = 0; i < nVals; i++)
                valList.Add(i * 1.5);

            DateTime timerStart = DateTime.Now;
            for (i = 0; i < nIter; i++)
            {
                TimeLabelBlob.Content = String.Format("Iteration {0}", i);
                tsLib.WriteValues(connNumber, "FileStrm2",
                           3, 1, nVals, valList, StartDate);
            }
            DateTime timerEnd = DateTime.Now;
            TimeSpan timerDiff = timerEnd - timerStart;
            TimeLabelBlob.Content = String.Format("BLOBWRI --- Iterations: {0};  Duration: {1:hh\\:mm\\:ss\\.f}", i, timerDiff);
        }

        
        
    }
}
