with Interfaces.C;

package Relax_Steps is
   function Prepare return Interfaces.C.int with Convention => C;
   function Run return Interfaces.C.int with Convention => C;
   function Publish return Interfaces.C.int with Convention => C;
end Relax_Steps;
