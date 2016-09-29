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
    public static class TSDateCalculator
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
            DateTime date = startDate;

            // The DateTime class's various 'Add' methods lend themselves to
            // a switch statement so that whichever units the time step is measured in,
            // we can call the corresponding 'Add' method.
            switch (unit)
            {
                case TimeStepUnitCode.Minute:
                    calcDate = date.AddMinutes(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Hour:
                    calcDate = date.AddHours(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Day:
                    calcDate = date.AddDays(stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Week:
                    calcDate = date.AddDays(stepSize * numSteps * 7);
                    break;
                case TimeStepUnitCode.Month:
                    // For now, we only handle monthly time steps that end on regular calendar month
                    // See https://github.com/hydrologics/Oasis/issues/163
                    date = date.RoundMonthEnd(0);
                    calcDate = date.AddMonthsByEnd(0, stepSize * numSteps);
                    break;
                case TimeStepUnitCode.Year:
                    // For now, we only handle yearly time steps that end on regular calendar year
                    // See https://github.com/hydrologics/Oasis/issues/163
                    date = new DateTime(date.Year + 1, 1, 1).AddMinutes(-1);
                    calcDate = date.AddYears(stepSize * numSteps);
                    break;
                default:
                    calcDate = date;
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
        /// Please note that the returned value is not inclusive of the first time step. That is, if the
        /// start date is 1/1 and the end date is 1/1, then the number of days between the given dates
        /// is returned as zero.
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
                    // For now, we only handle monthly time steps that end on regular calendar month
                    // See https://github.com/hydrologics/Oasis/issues/163
                    calcDate = RoundMonthEnd(calcDate, 0);
                    for (i = 0; calcDate < endDate; i++)
                        calcDate = calcDate.AddMonthsByEnd(0, stepSize);
                    break;
                case TimeStepUnitCode.Year:
                    // For now, we only handle yearly time steps that end on regular calendar year
                    // See https://github.com/hydrologics/Oasis/issues/163
                    calcDate = new DateTime(calcDate.Year + 1, 1, 1).AddMinutes(-1);
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
                    // For now, we do not handle monthly time steps that are shifted.  That is, the
                    // time steps must end on the end of the standard calendar months. Future work
                    // should make it possible to create shifted months only once a *shift* parameter
                    // is specified.  See https://github.com/hydrologics/Oasis/issues/163 . For now,
                    // the shift parameter is always assumed zero.
                    dateArray[0] = RoundMonthEnd(reqStartDate, 0);
                    for (int i = 1; i < nReqValues; i++)
                        dateArray[i] = dateArray[i - 1].AddMonthsByEnd(0, timeStepQuantity);
                    break;
                case TimeStepUnitCode.Year:
                    // For now, we do not handle yearly time steps that are shifted.  That is, the
                    // time steps must end on the end of the standard calendar year. Future work
                    // should make it possible to create shifted years only once a *shift* parameter
                    // is specified.  See https://github.com/hydrologics/Oasis/issues/163 . For now,
                    // the shift parameter is always assumed zero.
                    dateArray[0] = new DateTime(reqStartDate.Year, 12, 31);
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

        // TODO %%%: an AddYearsByEnd method is also needed

        #region AddMonthsByEnd method
        /// <summary>
        /// This method returns a new DateTime value that adds the specified number of months to the]
        /// given value. The method assumes that months are identified by the date at the end of the month,
        /// and it allows the months by be identified by a time shift (e.g. all months end on the 15th).
        /// This method contrasts with the DateTime.AddMonths extension method, which assumes months are
        /// identified by the start, and which would given inconsistent results for any shifted month
        /// system.
        /// </summary>
        /// <param name="startDate">The date to which months are to be added</param>
        /// <param name="dayShift">The shift (measured in days) in the definition of monthly time steps.
        /// If the value is 0, then months are defined as ending on the last day of the standard calendar
        /// month.  If the value is 1, then months are defined as ending on the 1st day of the standard
        /// calendar month.  If the value is -1, then months are defined as ending on the day before the
        /// last day of the standard calendar month.</param>
        /// <param name="count">the number of months to add to the given date</param>
        public static DateTime AddMonthsByEnd(this DateTime startDate, int dayShift, int count)
        {
            // trivial case requires no math
            if (count == 0) return startDate;

            // The AddMonths method only gives proper results for a system where
            //  1) time step is identified by beginning of period, and
            //  2) time step begins on day 1 of month
            // Therefore, we take the start date that is assumed to be end of period, convert
            // to beginning of period, and remove the potential time shift in month definition.
            DateTime startOfNextMonth = startDate.AddDays(1 - dayShift);
            // Use AddMonths method to add the number of months requrested by this method's parameter
            DateTime endDate = startOfNextMonth.AddMonths(count);
            // Convert back to the end of period with potential time shift
            endDate = endDate.AddDays(dayShift - 1);

            return endDate;
        } 
        #endregion

        #region RoundMonthEnd method
        /// <summary>
        /// This method returns the end of the month that contains the given date.  The method has not
        /// been designed to work if the dayShift is outside the range -30 to +30.
        /// </summary>
        /// <param name="date">the date whose month end is to be returned</param>
        /// <param name="dayShift">The shift (measured in days) in the definition of monthly time steps.
        /// If the value is 0, then months are defined as ending on the last day of the standard calendar
        /// month.  If the value is 1, then months are defined as ending on the 1st day of the standard
        /// calendar month.  If the value is -1, then months are defined as ending on the day before the
        /// last day of the standard calendar month.</param>
        public static DateTime RoundMonthEnd(this DateTime date, int dayShift)
        {
            DateTime endDate = new DateTime(date.Year, date.Month, 1).AddMinutes(-1).AddDays(dayShift);
            while (endDate < date)
                endDate = endDate.AddMonthsByEnd(dayShift, 1);
            return endDate;
        }
        #endregion

    }
}
