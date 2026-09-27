with Ada.Strings.Unbounded;
with Interfaces.C;

package Httk_Workflow is
   package C renames Interfaces.C;
   package U renames Ada.Strings.Unbounded;

   HTTK_WORKFLOW_OK       : constant C.int := 0;
   HTTK_WORKFLOW_ABSENT   : constant C.int := 1;
   HTTK_WORKFLOW_REFUSED  : constant C.int := 2;
   HTTK_WORKFLOW_ADA_API_VERSION : constant := 2;

   type Step_Handler is access function return C.int with Convention => C;
   type Step_Handlers is array (Positive range <>) of Step_Handler;
   type Step_Names is array (Positive range <>) of U.Unbounded_String;
   type String_List is array (Positive range <>) of U.Unbounded_String;
   No_Arguments : constant String_List (1 .. 0) := (others => U.Null_Unbounded_String);

   function Httk_Workflow_Runner
     (Workflow : String; Names : Step_Names; Handlers : Step_Handlers) return C.int;
   function Httk_Workflow_Main return C.int;
   procedure Httk_Workflow_Describe;
   procedure Httk_Workflow_Exit (Status : C.int);

   function Httk_Workflow_Invoke
     (Arguments : String_List; Output : out U.Unbounded_String; Present : out Boolean) return C.int;
   function Httk_Workflow_Invoke (Arguments : String_List) return C.int;

   procedure Httk_Workflow_Context
     (Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Context
     (Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int;
      Field : String);
   procedure Httk_Workflow_Parameter
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Parameter
     (Name : String; Fallback : String; Value : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Setting
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Setting
     (Name : String; Fallback : String; Value : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Environment
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Environment
     (Name : String; Fallback : String; Value : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_State_Get
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Declaration
     (Name : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Children
     (Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Children
     (Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int;
      Selection : String);
   procedure Httk_Workflow_Child
     (Label : String; Field : String; Value : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int);

   function Httk_Workflow_State_Set (Name : String; Value : String) return C.int;
   function Httk_Workflow_State_Delete (Name : String) return C.int;
   function Httk_Workflow_State_Merge (Assignments : String_List) return C.int;
   function Httk_Workflow_Declare (Name : String; Document_File : String) return C.int;
   function Httk_Workflow_Runlog_Note (Message : String) return C.int;
   function Httk_Workflow_Runlog_Headline (Message : String) return C.int;
   function Httk_Workflow_Runlog_Append
     (Message : String; Files : String_List := No_Arguments) return C.int;
   function Httk_Workflow_Log (Level : String; Message : String) return C.int;

   procedure Httk_Workflow_Put
     (Source : String; Destination : String; Operation : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Remove
     (Destination : String; Operation : out U.Unbounded_String; Present : out Boolean;
      Status : out C.int; Missing_Ok : Boolean := False);
   procedure Httk_Workflow_Spawn
     (Label : String; Job_Key : out U.Unbounded_String; Present : out Boolean;
      Status : out C.int; Arguments : String_List := No_Arguments);

   function Httk_Workflow_Advance
     (Next_Step : String; Arguments : String_List := No_Arguments) return C.int;
   function Httk_Workflow_Gather
     (Next_Step : String; Arguments : String_List := No_Arguments) return C.int;
   function Httk_Workflow_Succeed return C.int;
   function Httk_Workflow_Fail
     (Code : String; Message : String; Arguments : String_List := No_Arguments) return C.int;
   function Httk_Workflow_Retry (Reason : String) return C.int;
   function Httk_Workflow_Pause (Reason : String) return C.int;
   function Httk_Workflow_Batch return C.int;

   procedure Httk_Workflow_Job_Prepare
     (Destination : String; Spec_File : String; Job : out U.Unbounded_String;
      Present : out Boolean; Status : out C.int);
   procedure Httk_Workflow_Workdir_Apply
     (Spec_File : String; Id : out U.Unbounded_String; Present : out Boolean; Status : out C.int);
   function Httk_Workflow_Run (Arguments : String_List) return C.int;
   procedure Httk_Calc
     (Expression : String; Value : out U.Unbounded_String; Present : out Boolean; Status : out C.int);
   function Httk_Template_Render
     (Template_File : String; Output : String; Values_File : String) return C.int;
   function Httk_Compress (Arguments : String_List) return C.int;
   function Httk_Decompress (Arguments : String_List) return C.int;
end Httk_Workflow;
