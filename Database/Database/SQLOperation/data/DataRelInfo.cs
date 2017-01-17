using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation.data
{
    public class DataRelInfo
    {
        public int recordSize;            // Size per row
        public int attrCount;             // # of attributes
        public int numPages;              // # of pages used by relation
        public int numRecords;            // # of records in relation
        public char[] relName;    // Relation name

        public static int size()
        {
            return (Const.ConstProperty.MAXSTRINGLEN + 1) + 4 * sizeof(int);
        }

        public static int members()
        {
            return 5;
        }

        public DataRelInfo()
        {
            relName = new char[Const.ConstProperty.MAXSTRINGLEN + 1];
        }

        public static char[] DataRelInfoToChar(DataRelInfo info)
        {

            return null;
        }

        public static DataRelInfo[] CharToDataRelInfo(char[] data)
        {

            return null;
        }
    }
}
