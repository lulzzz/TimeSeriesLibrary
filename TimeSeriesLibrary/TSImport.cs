﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class contains meta-data about one time series record that was imported.  This
    /// meta-data can be used by functions outside of TimeSeriesLibrary to finish processing
    /// the time series that were imported from an XML file by the TSXml class.  In this way,
    /// the outside functions can handle features of the time series that TimeSeriesLibrary
    /// was not designed to handle.
    /// </summary>
    public class TSImport
    {
        // These fields are filled in by both the detailed and non-detailed XML methods
        public String Name;
        public Guid Id;
        public String UnprocessedElements;

        // These fields are only filled by the detailed XML methods
        public String APart;
        public String BPart;
        public String CPart;
        public String EPart;
        public String Units;
        public String TimeSeriesType;
        public int TraceNumber;
        
        // These fields may be stored to the database by some XML methods,
        // and they are only filled into this object by the detailed XML methods.
        public TSDateCalculator.TimeStepUnitCode TimeStepUnit;
        public short TimeStepQuantity;
        public int TimeStepCount;
        public DateTime BlobStartDate;
        public DateTime BlobEndDate;
        public byte[] Checksum;
        public byte[] BlobData;


        #region Method TransferParametersIntoImportRecord()
        /// <summary>
        /// This method copies the meta parameters of a BLOB from a TSParameters object
        /// (which is designed to hold meta parameters) into a TSImport object (which
        /// is designed to record the meta parameters of the BLOB and other descriptors
        /// of the time series that were read from an XML file).
        /// </summary>
        /// <param name="tsi">The TSImport object that values will be copied into</param>
        /// <param name="tsp">The TSParameters object that values will be copied from</param>
        public void RecordFromTSParameters(TSParameters tsp)
        {
            TimeStepUnit = tsp.TimeStepUnit;
            TimeStepQuantity = tsp.TimeStepQuantity;
            BlobStartDate = tsp.BlobStartDate;
            BlobEndDate = tsp.BlobEndDate;
            TimeStepCount = tsp.TimeStepCount;
        }
        #endregion

    }
}
