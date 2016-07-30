using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class stores methods and enum for computations involving time step size.
    /// All of this class's methods are static, so the class does not need to be instantiated.
    /// </summary>
    public class TSDateCalculator
    {

        #region enum TimeStepUnitCode
        /// <summary>
        /// This enum expresses the possible values in the TimeStepUnit field of the database table
        /// </summary>
        public enum TimeStepUnitCode : short
        {
            Irregular = 0,
            Minute = 1,
            Hour = 2,
            Day = 3,
            Week = 4,
            Month = 5,
            Year = 6
        } 
        #endregion


        #region IncrementDate() method
        /// <summary>
        /// This method computes the end date when given the start date, a regular time step size,
        /// and the number of time steps that should be added to the start date.
        /// </summary>
        /// <param name="startDate">The start date for the calculation</param>
        /// <param name="unit">Code for the time step units (e.g. hour, day, month...)</param>
        /// <param name="stepSize">The number of units per time step (e.g. Size=10 for 10-day time steps)</param>
        /// <param name="numSteps">The number of time steps to add to the start date</param>
        /// <returns>The end date of the calculation</returns>
        public static DateTime IncrementDate(DateTime startDate, TimeStepUnitCode unit,
            short stepSize, int numSteps)
        {
            DateTime calcDate;  // The end date of the calculation

            // The DateTime class's various 'Add' methods lend themselves to
            // a switch statement so that whichever units the time step is measured in,
            // we can call the corresponding 'Add' method.
            switch (unit)
            {
                case TimeStepUnitCode.Minute:
                    calcDate = startDate.AddMinutes(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Hour:
                    calcDate = startDate.AddHours(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Day:
                    calcDate = startDate.AddDays(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Week:
                    calcDate = startDate.AddDays(stepSize * numSteps * 7);
                    break;
                case TimeStepUnitCode.Month:
                    calcDate = startDate.AddMonths(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Year:
                    calcDate = startDate.AddYears(stepSize * numSteps);
                    break;
                default:
                    calcDate = startDate;
                    break;
            }
            return calcDate;
        } 
        #endregion


        #region CountSteps() method
        /// <summary>
        /// This method determines how many regular time steps are contained between the given
        /// start date and end date, when given a regular time step size.
        /// The method does not care whether the input start date comes before the input end
        /// date.  It simply returns the absolute value of the number of steps between the given dates.
        /// </summary>
        /// <param name="startDate">The date when the count begins</param>
        /// <param name="endDate">The date when the count ends</param>
        /// <param name="unit">Code for the time step units (e.g. hour, day, month...)</param>
        /// <param name="stepSize">The number of units per time step (e.g. Size=10 for 10-day time steps)</param>
        /// <returns></returns>
        public static int CountSteps(DateTime startDate, DateTime endDate,
            TimeStepUnitCode unit, short stepSize)
        {
            DateTime calcDate;  // Date value that will be iterated for the counting process
            int i = 0;            // Counter for how many time steps

            // Method does not care whether the input start date comes before the input end
            // date.  It simply returns the absolute value of the number of steps between the given dates.
            if (endDate < startDate)
            {
                calcDate = endDate;
                endDate = startDate;
                startDate = calcDate;
            }
            calcDate = startDate;

            // The DateTime class's various 'Add' methods lend themselves to
            // a switch statement so that whichever units the time step is measured in,
            // we can call the corresponding 'Add' method.  The counting loop inside of
            // the switch statement is deliberately selected to be faster than a switch
            // statement inside of a loop.
            switch (unit)
            {
                case TimeStepUnitCode.Minute:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddMinutes(stepSize);
                    break;
                case TimeStepUnitCode.Hour:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddHours(stepSize);
                    break;
                case TimeStepUnitCode.Day:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddDays(stepSize);
                    break;
                case TimeStepUnitCode.Week:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddDays(stepSize * 7);
                    break;
                case TimeStepUnitCode.Month:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddMonths(stepSize);
                    break;
                case TimeStepUnitCode.Year:
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddYears(stepSize);
                    break;
            }
            return i;
        } 
        #endregion


        #region FillDateArray() method
        /// <summary>
        /// This method fills values into the given array of date values, according to the
        /// regular time step size specified by the given time step unit and quantity.  The
        /// array must have been allocated prior to calling this method, and the size of the
        /// array must be at least the given number of time steps to fill.
        /// </summary>
        /// <param name="timeStepUnit">code value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="nReqValues">the number of time steps requested to fill into the array</param>
        /// <param name="dateArray">the array of DateTime values that the method will fill</param>
        /// <param name="reqStartDate">the DateTime value of the first time step</param>
        public static void FillDateArray(
                    TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                    int nReqValues, DateTime[] dateArray, DateTime reqStartDate)
        {
            // Element zero of the array comes directly from the input parameter
            dateArray[0] = reqStartDate;

            TimeSpan span;
            // We will loop through the length of the array and fill in the date values. However, the action
            // to be performed within the loop corresponds to the time step unit. In a previous version of
            // the code, this method simply called IncrementDate, which provided satisfying encapsulation
            // of code (avoiding duplication of effort). However, this method was found to be a performance
            // problem, so the code now attempts to optimize by minimizing the calculations that occur
            // within the loop. Where possible, it also calls the DateTime.Add method, using a TimeSpan
            // parameter, which is faster than calling AddDays or other methods. This means that the
            // method is noticeably slower for the Month and Year units, but this is mitigated by the
            // fact that usually there are fewer time steps in a monthly or yearly series.
            switch (timeStepUnit)
            {
                case TimeStepUnitCode.Minute:
                    span = new TimeSpan(0, 0, timeStepQuantity, 0);
                    for (int i = 1; i < nReqValues; i++)
                        dateArray[i] = dateArray[i - 1].Add(span);
                    break;
                case TimeStepUnitCode.Hour:
                    span = new TimeSpan(0, timeStepQuantity, 0, 0);
                    for (int i = 1; i < nReqValues; i++)
                        dateArray[i] = dateArray[i - 1].Add(span);
                    break;
                case TimeStepUnitCode.Day:
                    span = new TimeSpan(timeStepQuantity, 0, 0, 0);
                    for (int i = 1; i < nReqValues; i++)
                        dateArray[i] = dateArray[i - 1].Add(span);
                    break;
                case TimeStepUnitCode.Week:
                    span = new TimeSpan(timeStepQuantity * 7, 0, 0, 0);
                    for (int i = 1; i < nReqValues; i++)
                        dateArray[i] = dateArray[i - 1].Add(span);
                    break;
                case TimeStepUnitCode.Month:
                    for (int i = 1; i < nReqValues; i++)
                        dateArray[i] = dateArray[i - 1].AddMonths(timeStepQuantity);
                    break;
                case TimeStepUnitCode.Year:
                    for (int i = 1; i < nReqValues; i++)
                        dateArray[i] = dateArray[i - 1].AddYears(timeStepQuantity);
                    break;
                default:
                    for (int i = 1; i < nReqValues; i++)
                        dateArray[i] = dateArray[i - 1];
                    break;
            }

        } 
        #endregion

    }
}
