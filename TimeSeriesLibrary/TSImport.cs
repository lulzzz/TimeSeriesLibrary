﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class data for one time series record that was imported.  This
    /// meta-data can be used by functions outside of TimeSeriesLibrary to finish processing
    /// the time series that were imported from an XML file by the TSXml class.  In this way,
    /// the outside functions can handle features of the time series that TimeSeriesLibrary
    /// was not designed to handle.
    /// </summary>
    public class TSImport
    {
        /// <summary>
        /// This value indicates whether the TSImport object records certain elements 
        /// from an XML file to dedicated fields within the TSImport object, or to the 
        /// UnprocessedElements string field of the TSImport object.  It also determines
        /// whether the TSImport object records the BLOB of timeseries data.
        /// 
        /// If true, then XML elements such as "Apart" are recorded to their own fields,
        /// and the BLOB is recorded in the BlobData field.
        /// 
        /// If false, then XML elements such as "Apart" are recorded to the 
        /// UnprocessedElements field, and the BLOB is not recorded in this object.
        /// </summary>
        public Boolean IsDetailed;


        // These fields are recorded to the TSImport object regardless of the value of IsDetailed.
        /// <summary>
        /// Content of the Name tag in the XML file, presumably the name of a HECDSS record
        /// </summary>
        public String Name;
        /// <summary>
        /// The ID that identifies the record that was created for this timeseries in 
        /// the the database.  If the database was not written to, then this field is meaningless.
        /// </summary>
        public int Id;
        /// <summary>
        /// A string of XML elements, including the surrounding tags, that were not directly processed
        /// by the TimeSeriesLibrary.  The design calls for the caller to process these elements
        /// according to its own logic.
        /// </summary>
        public String UnprocessedElements;
        /// <summary>
        /// If true, then this TSImport represents a cyclic Time Pattern
        /// </summary>
        public Boolean IsPattern = false;


        // These fields are only recorded in the TSImport object when IsDetailed==true.
        public String APart;
        public String BPart;
        public String CPart;
        public String EPart;
        public String Units;
        public String TimeSeriesType;
        public Double MultiplicationFactor = 1.0;
        
        // These fields are meta-parameters of the BLOB that must be recorded in a consistent
        // manner with the BLOB itself.  The fields are recorded to the TSImport object
        // regardless of the value of IsDetailed.
        public TSDateCalculator.TimeStepUnitCode TimeStepUnit;
        public short TimeStepQuantity;
        public DateTime BlobStartDate;
        public int CompressionCode;
        public byte[] Checksum;

        // This list contains an object for each trace of the time series.  Each object
        // in the list contains a trace number, a BLOB containing time series values, and a checksum.
        public List<ITimeSeriesTrace> TraceList = new List<ITimeSeriesTrace>();

        #region Class Constructor
        /// <summary>
        /// Class constructor for TSImport class.
        /// </summary>
        /// <param name="isDetailed">
        /// This value indicates whether the TSImport object records certain elements 
        /// from an XML file to dedicated fields within the TSImport object, or to the 
        /// UnprocessedElements string field of the TSImport object.  It also determines
        /// whether the TSImport object records the BLOB of timeseries data.
        /// 
        /// If true, then XML elements such as "Apart" are recorded to their own fields,
        /// and the BLOB is recorded in the BlobData field.
        /// 
        /// If false, then XML elements such as "Apart" are recorded to the 
        /// UnprocessedElements field, and the BLOB is not recorded in this object.
        /// </param>
        public TSImport(Boolean isDetailed)
        {
            IsDetailed = isDetailed;
        }
        #endregion


        #region Methods for recording details
        // These methods are designed to make the TSXml class more readable, by hiding the logic
        // involved in setting these fields into the TSImport class.
        public void SetAPart(XmlReader xmlReader) { SetDetailFieldString(ref APart, xmlReader); }
        public void SetBPart(XmlReader xmlReader) { SetDetailFieldString(ref BPart, xmlReader); }
        public void SetCPart(XmlReader xmlReader) { SetDetailFieldString(ref CPart, xmlReader); }
        public void SetEPart(XmlReader xmlReader) { SetDetailFieldString(ref EPart, xmlReader); }
        public void SetUnits(XmlReader xmlReader) { SetDetailFieldString(ref Units, xmlReader); }
        public void SetTimeSeriesType(XmlReader xmlReader) { SetDetailFieldString(ref TimeSeriesType, xmlReader); }
        public void SetMultiplicationFactor(XmlReader xmlReader) 
        { 
            SetDetailFieldDouble(ref MultiplicationFactor, xmlReader, 1.0); 
        }

        public int GetTraceNumber(XmlReader xmlReader)
        {
            String elementString = xmlReader.ReadOuterXml();
            if (IsDetailed == false)
                AddUnprocessedElement(elementString);

            var xElement = XElement.Parse(elementString);
            String s = xElement.Value;
            int traceNumber;
            if (int.TryParse(s, out traceNumber) == false)
                traceNumber = 1;

            return traceNumber;
        }

        // The field that is passed as a parameter will be assigned from the current XML 
        // element if IsDetailed is true.  Otherwise, the XML element is recorded in the 
        // UnprocessedElements field.
        private void SetDetailFieldString(ref String s, XmlReader xmlReader)
        {
            if (IsDetailed)
                s = xmlReader.ReadElementContentAsString();
            else
                AddUnprocessedElement(xmlReader.ReadOuterXml());
        }
        // The field that is passed as a parameter will be assigned from the current XML 
        // element if IsDetailed is true.  Otherwise, the XML element is recorded in the 
        // UnprocessedElements field.
        private void SetDetailFieldInt(ref int i, XmlReader xmlReader, int defaultVal)
        {
            String tagName = xmlReader.Name;
            String s = xmlReader.ReadElementContentAsString();

            if (IsDetailed)
            {
                if (s == String.Empty)
                    i = defaultVal;
                else
                    i = int.Parse(s);
            }
            else
                AddUnprocessedElement(xmlReader.ReadOuterXml());
        }
        // The field that is passed as a parameter will be assigned from the current XML 
        // element if IsDetailed is true.  Otherwise, the XML element is recorded in the 
        // UnprocessedElements field.
        private void SetDetailFieldDouble(ref Double x, XmlReader xmlReader, Double defaultVal)
        {
            String tagName = xmlReader.Name;
            String s = xmlReader.ReadElementContentAsString();

            if (IsDetailed)
            {
                if (s == String.Empty)
                    x = defaultVal;
                else
                    x = Double.Parse(s);
            }
            else
                AddUnprocessedElement(xmlReader.ReadOuterXml());
        }
        /// <summary>
        /// This method adds a string to the UnprocessedElements string.  
        /// The UnprocessedElements string stores XML elements that TimeSeriesLibrary
        /// does not recognize, so that the calling process can parse and process
        /// these elements.
        /// </summary>
        /// <param name="s">The string that is to be added to the UnprocessedElements string.
        /// The given string should include the enclosing XML tags.</param>
        public void AddUnprocessedElement(String s)
        {
            // A space is inserted between elements for better XML parsing
            UnprocessedElements += " " + s;
        }
        #endregion


        #region Method RecordFromTSParameters()
        /// <summary>
        /// This method copies the parameters of a time series from a TSParameters object
        /// into this TSImport object.
        /// </summary>
        /// <param name="tsp">The TSParameters object that values will be copied from</param>
        public void RecordFromTSParameters(TSParameters tsp)
        {
            TimeStepUnit = tsp.TimeStepUnit;
            TimeStepQuantity = tsp.TimeStepQuantity;
            BlobStartDate = tsp.BlobStartDate;
            CompressionCode = tsp.CompressionCode;
        }
        #endregion


        #region Method RecordFromTS()
        /// <summary>
        /// This method copies into this TSImport object:
        ///      the parameters of the time series
        ///      the checksum for the entire time series
        ///      the ID of the database record for the time series.
        /// </summary>
        /// <param name="tsp">The TS object that values will be copied from</param>
        public void RecordFromTS(TS ts)
        {
            Id = ts.Id;
            RecordFromTSParameters(ts.TSParameters);
            Checksum = ts.Checksum;
        }
        #endregion

    }
}
