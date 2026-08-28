with Ada.Directories;
with Ada.Environment_Variables;
with Ada.Streams;
with Ada.Streams.Stream_IO;
with Ada.Strings.Unbounded;
with Httk_Workflow;

package body Relax_Steps is
   package C renames Interfaces.C;
   package U renames Ada.Strings.Unbounded;
   use type C.int;
   use type Ada.Streams.Stream_Element_Offset;

   procedure Ignore (Value : C.int) is
      pragma Unreferenced (Value);
   begin
      null;
   end Ignore;

   Collect : constant array (Positive range 1 .. 7) of U.Unbounded_String :=
     (U.To_Unbounded_String ("INCAR"), U.To_Unbounded_String ("KPOINTS"),
      U.To_Unbounded_String ("OUTCAR"), U.To_Unbounded_String ("CONTCAR"),
      U.To_Unbounded_String ("OSZICAR"), U.To_Unbounded_String ("vasprun.xml"),
      U.To_Unbounded_String ("vasp-run-report.json"));

   function Env (Name : String; Fallback : String) return String is
   begin
      return Ada.Environment_Variables.Value (Name, Fallback);
   end Env;

   function Copy_File (Source : String; Destination : String) return Boolean is
      Input : Ada.Streams.Stream_IO.File_Type;
      Output : Ada.Streams.Stream_IO.File_Type;
      Buffer : Ada.Streams.Stream_Element_Array (1 .. 8192);
      Last : Ada.Streams.Stream_Element_Offset;
   begin
      Ada.Streams.Stream_IO.Open (Input, Ada.Streams.Stream_IO.In_File, Source);
      Ada.Streams.Stream_IO.Create (Output, Ada.Streams.Stream_IO.Out_File, Destination);
      while not Ada.Streams.Stream_IO.End_Of_File (Input) loop
         Ada.Streams.Stream_IO.Read (Input, Buffer, Last);
         if Last >= Buffer'First then
            Ada.Streams.Stream_IO.Write (Output, Buffer (Buffer'First .. Last));
         end if;
      end loop;
      Ada.Streams.Stream_IO.Close (Input);
      Ada.Streams.Stream_IO.Close (Output);
      return True;
   exception
      when others =>
         if Ada.Streams.Stream_IO.Is_Open (Input) then Ada.Streams.Stream_IO.Close (Input); end if;
         if Ada.Streams.Stream_IO.Is_Open (Output) then Ada.Streams.Stream_IO.Close (Output); end if;
         return False;
   end Copy_File;

   function Stage_Input
     (Job_Dir : String; Parameter : String; Fallback : String; Destination : String) return Integer is
      Relative : U.Unbounded_String;
      Present : Boolean;
      Status : C.int;
      Source : U.Unbounded_String;
   begin
      Httk_Workflow.Httk_Workflow_Parameter (Parameter, Fallback, Relative, Present, Status);
      if Status /= Httk_Workflow.HTTK_WORKFLOW_OK or else not Present then return -1; end if;
      Source := U.To_Unbounded_String (Job_Dir & "/" & U.To_String (Relative));
      if not Ada.Directories.Exists (U.To_String (Source)) then return 0; end if;
      if Copy_File (U.To_String (Source), Destination) then return 1; end if;
      return -1;
   end Stage_Input;

   function Is_Space (Value : Character) return Boolean is
   begin
      return Value = ' ' or else Value = Character'Val (9);
   end Is_Space;

   function Token_Count (Command : String) return Natural is
      Count : Natural := 0;
      In_Token : Boolean := False;
   begin
      for I in Command'Range loop
         if Is_Space (Command (I)) then
            In_Token := False;
         elsif not In_Token then
            Count := Count + 1;
            In_Token := True;
         end if;
      end loop;
      return Count;
   end Token_Count;

   function Run_Arguments (Timeout : String; Command : String) return Httk_Workflow.String_List is
      Tokens : constant Natural := Token_Count (Command);
      Result : Httk_Workflow.String_List (1 .. 5 + Tokens);
      Position : Positive := 6;
      Start : Positive := Command'First;
      In_Token : Boolean := False;
   begin
      Result (1) := U.To_Unbounded_String ("--timeout");
      Result (2) := U.To_Unbounded_String (Timeout);
      Result (3) := U.To_Unbounded_String ("--report");
      Result (4) := U.To_Unbounded_String ("vasp-run-report.json");
      Result (5) := U.To_Unbounded_String ("--");
      for I in Command'Range loop
         if Is_Space (Command (I)) then
            if In_Token then
               Result (Position) := U.To_Unbounded_String (Command (Start .. I - 1));
               Position := Position + 1;
               In_Token := False;
            end if;
         elsif not In_Token then
            Start := I;
            In_Token := True;
         end if;
      end loop;
      if In_Token then Result (Position) := U.To_Unbounded_String (Command (Start .. Command'Last)); end if;
      return Result;
   end Run_Arguments;

   function Prepare return C.int is
      Job_Dir : constant String := Env ("HTTK_WORKFLOW_JOB_DIR", ".");
      Staged : Integer;
   begin
      Staged := Stage_Input (Job_Dir, "poscar", "files/POSCAR", "POSCAR");
      if Staged <= 0 then
         Staged := Integer (Httk_Workflow.Httk_Workflow_Fail
           ("vasp.input_missing", "the starting structure is not in this payload"));
         return 0;
      end if;
      Ignore (C.int (Stage_Input (Job_Dir, "incar", "files/INCAR", "INCAR")));
      Ignore (Httk_Workflow.Httk_Workflow_Runlog_Note ("prepared a relaxation"));
      Ignore (Httk_Workflow.Httk_Workflow_Advance ("run"));
      return 0;
   end Prepare;

   function Run return C.int is
      From_Parameter : U.Unbounded_String;
      Command : U.Unbounded_String;
      Timeout : U.Unbounded_String;
      Present : Boolean;
      Status : C.int;
      Run_Status : C.int;
   begin
      Httk_Workflow.Httk_Workflow_Parameter ("vasp_command", "", From_Parameter, Present, Status);
      Httk_Workflow.Httk_Workflow_Setting ("vasp.command", U.To_String (From_Parameter), Command, Present, Status);
      if not Present or else U.Length (Command) = 0 then
         Status := Httk_Workflow.Httk_Workflow_Fail
           ("vasp.command_missing", "no VASP command is configured: set it with httk workspace settings set --key vasp.command --value '...' WORKSPACE, or set HTTK_VASP_COMMAND, or give the job a vasp_command parameter");
         return 0;
      end if;
      Httk_Workflow.Httk_Workflow_Parameter ("timeout", "86400", Timeout, Present, Status);
      Run_Status := Httk_Workflow.Httk_Workflow_Run
        (Run_Arguments (U.To_String (Timeout), U.To_String (Command)));
      if Run_Status = 0 then
         Status := Httk_Workflow.Httk_Workflow_State_Set ("classification", "completed");
         Status := Httk_Workflow.Httk_Workflow_Runlog_Note ("VASP completed");
         Status := Httk_Workflow.Httk_Workflow_Advance ("publish");
      else
         Status := Httk_Workflow.Httk_Workflow_Fail
           ("vasp.failed", "VASP did not complete (nonzero status)");
      end if;
      return 0;
   end Run;

   function Publish return C.int is
      Prefix : U.Unbounded_String;
      Operation : U.Unbounded_String;
      Data_Dir : constant String := Env ("HTTK_WORKFLOW_DATA_DIR", "");
      Present : Boolean;
      Status : C.int;
   begin
      Httk_Workflow.Httk_Workflow_Parameter ("data_prefix", "vasp", Prefix, Present, Status);
      for I in Collect'Range loop
         if Ada.Directories.Exists (U.To_String (Collect (I))) and then Data_Dir'Length > 0 then
            Httk_Workflow.Httk_Workflow_Put
              (U.To_String (Collect (I)), U.To_String (Prefix) & "/" & U.To_String (Collect (I)),
               Operation, Present, Status);
         end if;
      end loop;
      if Data_Dir'Length > 0 then
         Status := Httk_Workflow.Httk_Workflow_Runlog_Note ("published to transactional data");
      else
         Status := Httk_Workflow.Httk_Workflow_Runlog_Note ("kept the result in the workdir");
      end if;
      Status := Httk_Workflow.Httk_Workflow_Succeed;
      return 0;
   end Publish;
end Relax_Steps;
