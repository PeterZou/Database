using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.Interface
{
    public interface PageHdr
    {
        void From_buf(char[] buf);

        char[] To_buf();

        int Size();

        int Mapsize();
    }
}
