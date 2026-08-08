with Ada.Strings.Unbounded;
with Interfaces.C;
with Httk_Workflow;
with Relax_Steps;

procedure Relax is
   package C renames Interfaces.C;
   package U renames Ada.Strings.Unbounded;
   use type C.int;

   Names : constant Httk_Workflow.Step_Names :=
     (U.To_Unbounded_String ("prepare"), U.To_Unbounded_String ("run"), U.To_Unbounded_String ("publish"));
   Handlers : constant Httk_Workflow.Step_Handlers :=
     (Relax_Steps.Prepare'Access, Relax_Steps.Run'Access, Relax_Steps.Publish'Access);
   Status : C.int;
begin
   Status := Httk_Workflow.Httk_Workflow_Runner ("httk.vasp.relax-ada", Names, Handlers);
   if Status /= Httk_Workflow.HTTK_WORKFLOW_OK then Httk_Workflow.Httk_Workflow_Exit (2); end if;
   Httk_Workflow.Httk_Workflow_Exit (Httk_Workflow.Httk_Workflow_Main);
end Relax;
