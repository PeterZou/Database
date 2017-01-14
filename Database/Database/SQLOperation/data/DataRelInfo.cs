using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation.data
{
    public class DataRelInfo
    {
        int recordSize;            // Size per row
        int attrCount;             // # of attributes
        int numPages;              // # of pages used by relation
        int numRecords;            // # of records in relation
        char[] relName;    // Relation name

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
    }
}
