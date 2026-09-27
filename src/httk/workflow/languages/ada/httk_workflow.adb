with Ada.Command_Line;
with Interfaces.C.Strings;
with System;
with Ada.Unchecked_Deallocation;

package body Httk_Workflow is
   package CS renames Interfaces.C.Strings;
   use type CS.chars_ptr;

   type C_String_Array is array (Natural range <>) of CS.chars_ptr with Convention => C;
   type C_Step is record
      Name : CS.chars_ptr;
      Handler : Step_Handler;
   end record with Convention => C;
   type C_Step_Array is array (Positive range <>) of C_Step with Convention => C;
   type C_Step_Array_Access is access all C_Step_Array;
   type C_Name_Array is array (Positive range <>) of CS.chars_ptr;
   type C_Name_Array_Access is access all C_Name_Array;

   procedure Free_Steps is new Ada.Unchecked_Deallocation (C_Step_Array, C_Step_Array_Access);
   procedure Free_Names is new Ada.Unchecked_Deallocation (C_Name_Array, C_Name_Array_Access);

   G_Workflow : CS.chars_ptr := CS.Null_Ptr;
   G_Steps : C_Step_Array_Access;
   G_Names : C_Name_Array_Access;

   procedure C_Free (Value : CS.chars_ptr)
     with Import, Convention => C, External_Name => "free";
   function C_Runner (Workflow : CS.chars_ptr; Steps : System.Address; Count : C.size_t) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_runner";
   function C_Main (Argc : C.int; Argv : System.Address) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_main";
   procedure C_Describe
     with Import, Convention => C, External_Name => "httk_workflow_describe";
   function C_Invoke (Output : access CS.chars_ptr; Arguments : System.Address) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_invoke";
   function C_Context (Field : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_context";
   function C_Parameter (Name, Fallback : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_parameter";
   function C_Setting (Name, Fallback : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_setting";
   function C_Environment (Name, Fallback : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_environment";
   function C_State_Get (Name : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_state_get";
   function C_Declaration (Name : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_declaration";
   function C_Children (Selection : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_children";
   function C_Child (Label, Field : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_child";
   function C_State_Set (Name, Value : CS.chars_ptr) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_state_set";
   function C_State_Delete (Name : CS.chars_ptr) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_state_delete";
   function C_State_Merge (Assignments : System.Address) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_state_merge";
   function C_Declare (Name, File : CS.chars_ptr) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_declare";
   function C_Runlog_Note (Message : CS.chars_ptr) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_runlog_note";
   function C_Runlog_Headline (Message : CS.chars_ptr) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_runlog_headline";
   function C_Runlog_Append (Message : CS.chars_ptr; Files : System.Address) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_runlog_append";
   function C_Log (Level, Message : CS.chars_ptr) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_log";
   function C_Put (Source, Destination : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_put";
   function C_Remove (Destination : CS.chars_ptr; Missing_Ok : C.int; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_remove";
   function C_Spawn (Label : CS.chars_ptr; Arguments : System.Address; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_spawn";
   function C_Advance (Next_Step : CS.chars_ptr; Arguments : System.Address) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_advance";
   function C_Gather (Next_Step : CS.chars_ptr; Arguments : System.Address) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_gather";
   function C_Succeed return C.int
     with Import, Convention => C, External_Name => "httk_workflow_succeed";
   function C_Fail (Code, Message : CS.chars_ptr; Arguments : System.Address) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_fail";
   function C_Retry (Reason : CS.chars_ptr) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_retry";
   function C_Pause (Reason : CS.chars_ptr) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_pause";
   function C_Batch return C.int
     with Import, Convention => C, External_Name => "httk_workflow_batch";
   function C_Job_Prepare (Destination, Spec_File : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_job_prepare";
   function C_Workdir_Apply (Spec_File : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_workflow_workdir_apply";
   function C_Run (Arguments : System.Address) return C.int
     with Import, Convention => C, External_Name => "httk_workflow_run";
   function C_Calc (Expression : CS.chars_ptr; Status : access C.int) return CS.chars_ptr
     with Import, Convention => C, External_Name => "httk_calc";
   function C_Template (Template_File, Output, Values_File : CS.chars_ptr) return C.int
     with Import, Convention => C, External_Name => "httk_template_render";
   function C_Compress (Arguments : System.Address) return C.int
     with Import, Convention => C, External_Name => "httk_compress";
   function C_Decompress (Arguments : System.Address) return C.int
     with Import, Convention => C, External_Name => "httk_decompress";
   procedure C_Exit (Status : C.int)
     with Import, Convention => C, External_Name => "exit";

   function New_Input (Value : String) return CS.chars_ptr is
   begin
      return CS.New_String (Value);
   end New_Input;

   procedure Release (Value : in out CS.chars_ptr) is
   begin
      if Value /= CS.Null_Ptr then
         CS.Free (Value);
      end if;
   end Release;

   procedure Take
     (Pointer : CS.chars_ptr; Value : out U.Unbounded_String; Present : out Boolean) is
   begin
      if Pointer = CS.Null_Ptr then
         Value := U.Null_Unbounded_String;
         Present := False;
      else
         Value := U.To_Unbounded_String (CS.Value (Pointer));
         Present := True;
         C_Free (Pointer);
      end if;
   end Take;

   procedure Prepare_Arguments (Arguments : String_List; Pointers : out C_String_Array) is
   begin
      for I in Arguments'Range loop
         Pointers (I - Arguments'First) := New_Input (U.To_String (Arguments (I)));
      end loop;
      Pointers (Pointers'Last) := CS.Null_Ptr;
   end Prepare_Arguments;

   procedure Release_Arguments (Pointers : in out C_String_Array) is
   begin
      for I in Pointers'First .. Pointers'Last - 1 loop
         Release (Pointers (I));
      end loop;
   end Release_Arguments;

   function Arguments_Address (Pointers : C_String_Array) return System.Address is
   begin
      if Pointers'Length = 1 then return System.Null_Address; end if;
      return Pointers'Address;
   end Arguments_Address;

   function Read_With_Pointer
     (Pointer : CS.chars_ptr; Status : C.int; Value : out U.Unbounded_String;
      Present : out Boolean) return C.int is
   begin
      Take (Pointer, Value, Present);
      return Status;
   end Read_With_Pointer;

   function Httk_Workflow_Runner
     (Workflow : String; Names : Step_Names; Handlers : Step_Handlers) return C.int is
   begin
      if Names'Length /= Handlers'Length or else Names'Length = 0 then
         return HTTK_WORKFLOW_REFUSED;
      end if;
      Release (G_Workflow);
      if G_Steps /= null then Free_Steps (G_Steps); end if;
      if G_Names /= null then
         for I in G_Names'Range loop Release (G_Names (I)); end loop;
         Free_Names (G_Names);
      end if;
      G_Workflow := New_Input (Workflow);
      G_Names := new C_Name_Array (Names'Range);
      G_Steps := new C_Step_Array (Names'Range);
      for I in Names'Range loop
         G_Names (I) := New_Input (U.To_String (Names (I)));
         G_Steps (I) :=
           (Name => G_Names (I), Handler => Handlers (Handlers'First + (I - Names'First)));
      end loop;
      return C_Runner (G_Workflow, G_Steps.all'Address, C.size_t (Names'Length));
   end Httk_Workflow_Runner;

   function Httk_Workflow_Main return C.int is
      Count : constant Natural := Ada.Command_Line.Argument_Count;
      Pointers : C_String_Array (0 .. Count + 1);
      Result : C.int;
   begin
      Pointers (0) := New_Input (Ada.Command_Line.Command_Name);
      for I in 1 .. Count loop
         Pointers (I) := New_Input (Ada.Command_Line.Argument (I));
      end loop;
      Pointers (Count + 1) := CS.Null_Ptr;
      Result := C_Main (C.int (Count + 1), Pointers'Address);
      Release_Arguments (Pointers);
      return Result;
   end Httk_Workflow_Main;

   procedure Httk_Workflow_Describe is
   begin
      C_Describe;
   end Httk_Workflow_Describe;

   procedure Httk_Workflow_Exit (Status : C.int) is
   begin
      C_Exit (Status);
   end Httk_Workflow_Exit;

   function Httk_Workflow_Invoke
     (Arguments : String_List; Output : out U.Unbounded_String; Present : out Boolean) return C.int is
      Pointers : C_String_Array (0 .. Arguments'Length);
      Captured : aliased CS.chars_ptr := CS.Null_Ptr;
      Result : C.int;
   begin
      Prepare_Arguments (Arguments, Pointers);
      Result := C_Invoke (Captured'Access, Pointers'Address);
      Take (Captured, Output, Present);
      Release_Arguments (Pointers);
      return Result;
   end Httk_Workflow_Invoke;

   function Httk_Workflow_Invoke (Arguments : String_List) return C.int is
      Pointers : C_String_Array (0 .. Arguments'Length);
      Result : C.int;
   begin
      Prepare_Arguments (Arguments, Pointers);
      Result := C_Invoke (null, Pointers'Address);
      Release_Arguments (Pointers);
      return Result;
   end Httk_Workflow_Invoke;

   procedure Httk_Workflow_Context
     (Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int) is
      C_Status : aliased C.int;
      Pointer : CS.chars_ptr;
   begin
      Pointer := C_Context (CS.Null_Ptr, C_Status'Access);
      Status := Read_With_Pointer (Pointer, C_Status, Value, Present);
   end Httk_Workflow_Context;

   procedure Httk_Workflow_Context
     (Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int;
      Field : String) is
      Input : CS.chars_ptr := New_Input (Field);
      C_Status : aliased C.int;
      Pointer : CS.chars_ptr;
   begin
      Pointer := C_Context (Input, C_Status'Access);
      Release (Input);
      Status := Read_With_Pointer (Pointer, C_Status, Value, Present);
   end Httk_Workflow_Context;

   procedure Read_Named
     (Name, Fallback : String; Has_Fallback : Boolean; Value : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int; Kind : Character) is
      B_Name : CS.chars_ptr := New_Input (Name);
      B_Fallback : CS.chars_ptr := CS.Null_Ptr;
      C_Status : aliased C.int;
      Pointer : CS.chars_ptr;
   begin
      if Has_Fallback then B_Fallback := New_Input (Fallback); end if;
      case Kind is
         when 'p' => Pointer := C_Parameter (B_Name, B_Fallback, C_Status'Access);
         when 's' => Pointer := C_Setting (B_Name, B_Fallback, C_Status'Access);
         when others => Pointer := C_Environment (B_Name, B_Fallback, C_Status'Access);
      end case;
      Release (B_Name); Release (B_Fallback);
      Status := Read_With_Pointer (Pointer, C_Status, Value, Present);
   end Read_Named;

   procedure Httk_Workflow_Parameter
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int) is
   begin Read_Named (Name, "", False, Value, Present, Status, 'p'); end Httk_Workflow_Parameter;
   procedure Httk_Workflow_Parameter
     (Name : String; Fallback : String; Value : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int) is
   begin Read_Named (Name, Fallback, True, Value, Present, Status, 'p'); end Httk_Workflow_Parameter;
   procedure Httk_Workflow_Setting
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int) is
   begin Read_Named (Name, "", False, Value, Present, Status, 's'); end Httk_Workflow_Setting;
   procedure Httk_Workflow_Setting
     (Name : String; Fallback : String; Value : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int) is
   begin Read_Named (Name, Fallback, True, Value, Present, Status, 's'); end Httk_Workflow_Setting;
   procedure Httk_Workflow_Environment
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int) is
   begin Read_Named (Name, "", False, Value, Present, Status, 'e'); end Httk_Workflow_Environment;
   procedure Httk_Workflow_Environment
     (Name : String; Fallback : String; Value : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int) is
   begin Read_Named (Name, Fallback, True, Value, Present, Status, 'e'); end Httk_Workflow_Environment;

   procedure Read_One
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int;
      Kind : Character) is
      Input : CS.chars_ptr := New_Input (Name);
      C_Status : aliased C.int;
      Pointer : CS.chars_ptr;
   begin
      case Kind is
         when 'g' => Pointer := C_State_Get (Input, C_Status'Access);
         when others => Pointer := C_Declaration (Input, C_Status'Access);
      end case;
      Release (Input);
      Status := Read_With_Pointer (Pointer, C_Status, Value, Present);
   end Read_One;
   procedure Httk_Workflow_State_Get
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int) is
   begin Read_One (Name, Value, Present, Status, 'g'); end Httk_Workflow_State_Get;
   procedure Httk_Workflow_Declaration
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int) is
   begin Read_One (Name, Value, Present, Status, 'd'); end Httk_Workflow_Declaration;

   procedure Httk_Workflow_Children
     (Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int) is
      C_Status : aliased C.int;
      Pointer : CS.chars_ptr;
   begin
      Pointer := C_Children (CS.Null_Ptr, C_Status'Access);
      Status := Read_With_Pointer (Pointer, C_Status, Value, Present);
   end Httk_Workflow_Children;

   procedure Httk_Workflow_Children
     (Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int;
      Selection : String) is
      Input : CS.chars_ptr := New_Input (Selection);
      C_Status : aliased C.int;
      Pointer : CS.chars_ptr;
   begin
      Pointer := C_Children (Input, C_Status'Access); Release (Input);
      Status := Read_With_Pointer (Pointer, C_Status, Value, Present);
   end Httk_Workflow_Children;

   procedure Httk_Workflow_Child
     (Label : String; Field : String; Value : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int) is
      B_Label : CS.chars_ptr := New_Input (Label);
      B_Field : CS.chars_ptr := New_Input (Field);
      C_Status : aliased C.int;
      Pointer : CS.chars_ptr;
   begin
      Pointer := C_Child (B_Label, B_Field, C_Status'Access);
      Release (B_Label); Release (B_Field);
      Status := Read_With_Pointer (Pointer, C_Status, Value, Present);
   end Httk_Workflow_Child;

   function Httk_Workflow_State_Set (Name : String; Value : String) return C.int is
      B_Name : CS.chars_ptr := New_Input (Name); B_Value : CS.chars_ptr := New_Input (Value);
      Result : C.int;
   begin Result := C_State_Set (B_Name, B_Value); Release (B_Name); Release (B_Value); return Result; end Httk_Workflow_State_Set;
   function Httk_Workflow_State_Delete (Name : String) return C.int is
      Input : CS.chars_ptr := New_Input (Name); Result : C.int;
   begin Result := C_State_Delete (Input); Release (Input); return Result; end Httk_Workflow_State_Delete;

   function Httk_Workflow_State_Merge (Assignments : String_List) return C.int is
      Pointers : C_String_Array (0 .. Assignments'Length); Result : C.int;
   begin Prepare_Arguments (Assignments, Pointers); Result := C_State_Merge (Arguments_Address (Pointers)); Release_Arguments (Pointers); return Result; end Httk_Workflow_State_Merge;
   function Httk_Workflow_Declare (Name : String; Document_File : String) return C.int is
      B_Name : CS.chars_ptr := New_Input (Name); B_File : CS.chars_ptr := New_Input (Document_File); Result : C.int;
   begin Result := C_Declare (B_Name, B_File); Release (B_Name); Release (B_File); return Result; end Httk_Workflow_Declare;
   function Httk_Workflow_Runlog_Note (Message : String) return C.int is
      Input : CS.chars_ptr := New_Input (Message); Result : C.int;
   begin Result := C_Runlog_Note (Input); Release (Input); return Result; end Httk_Workflow_Runlog_Note;
   function Httk_Workflow_Runlog_Headline (Message : String) return C.int is
      Input : CS.chars_ptr := New_Input (Message); Result : C.int;
   begin Result := C_Runlog_Headline (Input); Release (Input); return Result; end Httk_Workflow_Runlog_Headline;
   function Httk_Workflow_Runlog_Append (Message : String; Files : String_List := No_Arguments) return C.int is
      B_Message : CS.chars_ptr := New_Input (Message); Pointers : C_String_Array (0 .. Files'Length); Result : C.int;
   begin Prepare_Arguments (Files, Pointers); Result := C_Runlog_Append (B_Message, Arguments_Address (Pointers)); Release (B_Message); Release_Arguments (Pointers); return Result; end Httk_Workflow_Runlog_Append;
   function Httk_Workflow_Log (Level : String; Message : String) return C.int is
      B_Level : CS.chars_ptr := New_Input (Level); B_Message : CS.chars_ptr := New_Input (Message); Result : C.int;
   begin Result := C_Log (B_Level, B_Message); Release (B_Level); Release (B_Message); return Result; end Httk_Workflow_Log;

   procedure Httk_Workflow_Put
     (Source : String; Destination : String; Operation : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int) is
      B_Source : CS.chars_ptr := New_Input (Source); B_Destination : CS.chars_ptr := New_Input (Destination);
      C_Status : aliased C.int; Pointer : CS.chars_ptr;
   begin Pointer := C_Put (B_Source, B_Destination, C_Status'Access); Release (B_Source); Release (B_Destination); Status := Read_With_Pointer (Pointer, C_Status, Operation, Present); end Httk_Workflow_Put;
   procedure Httk_Workflow_Remove
     (Destination : String; Operation : out U.Unbounded_String; Present : out Boolean;
      Status : out C.int; Missing_Ok : Boolean := False) is
      B_Destination : CS.chars_ptr := New_Input (Destination); C_Status : aliased C.int; Pointer : CS.chars_ptr;
   begin Pointer := C_Remove (B_Destination, (if Missing_Ok then 1 else 0), C_Status'Access); Release (B_Destination); Status := Read_With_Pointer (Pointer, C_Status, Operation, Present); end Httk_Workflow_Remove;
   procedure Httk_Workflow_Spawn
     (Label : String; Job_Key : out U.Unbounded_String; Present : out Boolean;
      Status : out C.int; Arguments : String_List := No_Arguments) is
      B_Label : CS.chars_ptr := New_Input (Label); Pointers : C_String_Array (0 .. Arguments'Length);
      C_Status : aliased C.int; Pointer : CS.chars_ptr;
   begin Prepare_Arguments (Arguments, Pointers); Pointer := C_Spawn (B_Label, Arguments_Address (Pointers), C_Status'Access); Release (B_Label); Release_Arguments (Pointers); Status := Read_With_Pointer (Pointer, C_Status, Job_Key, Present); end Httk_Workflow_Spawn;

   function Httk_Workflow_Advance (Next_Step : String; Arguments : String_List := No_Arguments) return C.int is
      B_Next : CS.chars_ptr := New_Input (Next_Step); Pointers : C_String_Array (0 .. Arguments'Length); Result : C.int;
   begin Prepare_Arguments (Arguments, Pointers); Result := C_Advance (B_Next, Arguments_Address (Pointers)); Release (B_Next); Release_Arguments (Pointers); return Result; end Httk_Workflow_Advance;
   function Httk_Workflow_Gather (Next_Step : String; Arguments : String_List := No_Arguments) return C.int is
      B_Next : CS.chars_ptr := New_Input (Next_Step); Pointers : C_String_Array (0 .. Arguments'Length); Result : C.int;
   begin Prepare_Arguments (Arguments, Pointers); Result := C_Gather (B_Next, Arguments_Address (Pointers)); Release (B_Next); Release_Arguments (Pointers); return Result; end Httk_Workflow_Gather;
   function Httk_Workflow_Succeed return C.int is begin return C_Succeed; end Httk_Workflow_Succeed;
   function Httk_Workflow_Fail (Code : String; Message : String; Arguments : String_List := No_Arguments) return C.int is
      B_Code : CS.chars_ptr := New_Input (Code); B_Message : CS.chars_ptr := New_Input (Message); Pointers : C_String_Array (0 .. Arguments'Length); Result : C.int;
   begin Prepare_Arguments (Arguments, Pointers); Result := C_Fail (B_Code, B_Message, Arguments_Address (Pointers)); Release (B_Code); Release (B_Message); Release_Arguments (Pointers); return Result; end Httk_Workflow_Fail;
   function Httk_Workflow_Retry (Reason : String) return C.int is
      Input : CS.chars_ptr := New_Input (Reason); Result : C.int;
   begin Result := C_Retry (Input); Release (Input); return Result; end Httk_Workflow_Retry;
   function Httk_Workflow_Pause (Reason : String) return C.int is
      Input : CS.chars_ptr := New_Input (Reason); Result : C.int;
   begin Result := C_Pause (Input); Release (Input); return Result; end Httk_Workflow_Pause;
   function Httk_Workflow_Batch return C.int is begin return C_Batch; end Httk_Workflow_Batch;

   procedure Httk_Workflow_Job_Prepare
     (Destination : String; Spec_File : String; Job : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int) is
      B_Destination : CS.chars_ptr := New_Input (Destination); B_Spec : CS.chars_ptr := New_Input (Spec_File);
      C_Status : aliased C.int; Pointer : CS.chars_ptr;
   begin Pointer := C_Job_Prepare (B_Destination, B_Spec, C_Status'Access); Release (B_Destination); Release (B_Spec); Status := Read_With_Pointer (Pointer, C_Status, Job, Present); end Httk_Workflow_Job_Prepare;
   procedure Httk_Workflow_Workdir_Apply
     (Spec_File : String; Id : out U.Unbounded_String; Present : out Boolean; Status : out C.int) is
      B_Spec : CS.chars_ptr := New_Input (Spec_File); C_Status : aliased C.int; Pointer : CS.chars_ptr;
   begin Pointer := C_Workdir_Apply (B_Spec, C_Status'Access); Release (B_Spec); Status := Read_With_Pointer (Pointer, C_Status, Id, Present); end Httk_Workflow_Workdir_Apply;
   function Httk_Workflow_Run (Arguments : String_List) return C.int is
      Pointers : C_String_Array (0 .. Arguments'Length); Result : C.int;
   begin Prepare_Arguments (Arguments, Pointers); Result := C_Run (Arguments_Address (Pointers)); Release_Arguments (Pointers); return Result; end Httk_Workflow_Run;
   procedure Httk_Calc
     (Expression : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int) is
      Input : CS.chars_ptr := New_Input (Expression); C_Status : aliased C.int; Pointer : CS.chars_ptr;
   begin Pointer := C_Calc (Input, C_Status'Access); Release (Input); Status := Read_With_Pointer (Pointer, C_Status, Value, Present); end Httk_Calc;
   function Httk_Template_Render (Template_File : String; Output : String; Values_File : String) return C.int is
      B_Template : CS.chars_ptr := New_Input (Template_File); B_Output : CS.chars_ptr := New_Input (Output); B_Values : CS.chars_ptr := New_Input (Values_File); Result : C.int;
   begin Result := C_Template (B_Template, B_Output, B_Values); Release (B_Template); Release (B_Output); Release (B_Values); return Result; end Httk_Template_Render;
   function Httk_Compress (Arguments : String_List) return C.int is
      Pointers : C_String_Array (0 .. Arguments'Length); Result : C.int;
   begin Prepare_Arguments (Arguments, Pointers); Result := C_Compress (Arguments_Address (Pointers)); Release_Arguments (Pointers); return Result; end Httk_Compress;
   function Httk_Decompress (Arguments : String_List) return C.int is
      Pointers : C_String_Array (0 .. Arguments'Length); Result : C.int;
   begin Prepare_Arguments (Arguments, Pointers); Result := C_Decompress (Arguments_Address (Pointers)); Release_Arguments (Pointers); return Result; end Httk_Decompress;
end Httk_Workflow;
