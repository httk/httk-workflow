! One VASP relaxation, authored in modern Fortran: the same three-step shape as
! examples/relax_c/relax.c, built on the native Fortran SDK (native/fortran).
!
!   prepare  stage the payload POSCAR (and INCAR if present) into the workdir
!   run      run the configured VASP command and classify what it did
!   publish  copy the finished calculation into the job's transactional data
!
! Every httk_workflow_* call reaches the same C bridge the Python, Bash, and C
! SDKs do, so this runner is mock-vasp compatible and publishes the same bytes.
! Build it with the Makefile beside this file:
!
!     make
!
! See ../mock_vasp.py for a stand-in VASP, and README.md for the whole flow.

module relax_steps

  use, intrinsic :: iso_c_binding, only: c_int
  use, intrinsic :: iso_fortran_env, only: error_unit
  use httk_workflow

  implicit none
  private
  public :: step_prepare, step_run, step_publish

  ! The files a finished relaxation publishes, if the run produced them.
  character(len=*), parameter :: COLLECT(7) = [character(len=20) :: &
    "INCAR", "KPOINTS", "OUTCAR", "CONTCAR", "OSZICAR", "vasprun.xml", "vasp-run-report.json"]

  ! A generous fixed width for one run-argument field (a token of the command,
  ! or an option); wide enough for any realistic command path, and trailing
  ! blanks are stripped as the argument crosses the C boundary.
  integer, parameter :: ARG_WIDTH = 4096

contains

  ! One environment variable, or a default when it is unset or empty.
  function env(name, fallback) result(value)
    character(len=*), intent(in) :: name, fallback
    character(len=:), allocatable :: value
    integer :: l, s
    call get_environment_variable(name, length=l, status=s)
    if (s /= 0 .or. l == 0) then
      value = fallback
      return
    end if
    allocate (character(len=l) :: value)
    call get_environment_variable(name, value=value)
  end function

  logical function file_exists(path)
    character(len=*), intent(in) :: path
    inquire (file=path, exist=file_exists)
  end function

  ! Copy one file byte for byte; ok is .true. on success.
  subroutine copy_file(source, destination, ok)
    character(len=*), intent(in) :: source, destination
    logical, intent(out) :: ok
    character(len=:), allocatable :: buffer
    integer :: unit, ios
    integer(kind=8) :: nbytes
    ok = .false.
    inquire (file=source, size=nbytes)
    if (nbytes < 0) return
    open (newunit=unit, file=source, access="stream", form="unformatted", &
          status="old", action="read", iostat=ios)
    if (ios /= 0) return
    allocate (character(len=int(nbytes)) :: buffer)
    if (nbytes > 0) read (unit, iostat=ios) buffer
    close (unit)
    if (ios /= 0) return
    open (newunit=unit, file=destination, access="stream", form="unformatted", &
          status="replace", action="write", iostat=ios)
    if (ios /= 0) return
    if (nbytes > 0) write (unit, iostat=ios) buffer
    close (unit)
    ok = ios == 0
  end subroutine

  ! Stage a payload-relative file named by one parameter into the workdir.
  ! Returns 1 when staged, 0 when the source is absent, -1 on failure.
  function stage_input(job_dir, parameter, fallback, destination) result(result_code)
    character(len=*), intent(in) :: job_dir, parameter, fallback, destination
    integer :: result_code
    character(len=:), allocatable :: relative, source
    integer :: st
    logical :: ok
    ! The parameter has a fallback, so an OK read is always allocated; a refused
    ! bridge call (status /= OK) leaves `relative` unallocated and is a failure.
    call httk_workflow_parameter(parameter, relative, fallback, st)
    if (st /= HTTK_WORKFLOW_OK .or. .not. allocated(relative)) then
      result_code = -1
      return
    end if
    source = job_dir//"/"//relative
    if (.not. file_exists(source)) then
      result_code = 0
      return
    end if
    call copy_file(source, destination, ok)
    result_code = merge(1, -1, ok)
  end function

  function step_prepare() result(code) bind(c)
    integer(c_int) :: code
    character(len=:), allocatable :: job_dir
    integer :: staged
    job_dir = env("HTTK_WORKFLOW_JOB_DIR", ".")
    staged = stage_input(job_dir, "poscar", "files/POSCAR", "POSCAR")
    if (staged <= 0) then
      call ignore(httk_workflow_fail("vasp.input_missing", "the starting structure is not in this payload"))
      code = 0
      return
    end if
    ! An INCAR is optional; the mock VASP reads only the POSCAR.
    staged = stage_input(job_dir, "incar", "files/INCAR", "INCAR")
    call ignore(httk_workflow_runlog_note("prepared a relaxation"))
    call ignore(httk_workflow_advance("run"))
    code = 0
  end function

  function step_run() result(code) bind(c)
    integer(c_int) :: code
    character(len=:), allocatable :: from_parameter, command, timeout
    character(len=ARG_WIDTH), allocatable :: args(:)
    character(len=64) :: message
    integer :: run_status
    call httk_workflow_parameter("vasp_command", from_parameter, "")
    if (.not. allocated(from_parameter)) from_parameter = ""
    call httk_workflow_setting("vasp.command", command, from_parameter)
    if (.not. allocated(command)) command = ""
    if (len_trim(command) == 0) then
      call ignore(httk_workflow_fail("vasp.command_missing", &
        "no VASP command is configured: set it with "// &
        "httk workflow workspace settings set vasp.command '...', or set "// &
        "HTTK_VASP_COMMAND, or give the job a vasp_command parameter"))
      code = 0
      return
    end if
    call httk_workflow_parameter("timeout", timeout, "86400")
    if (.not. allocated(timeout)) timeout = "86400"
    call build_run_args(timeout, command, args)
    run_status = httk_workflow_run(args)
    if (run_status == 0) then
      call ignore(httk_workflow_state_set("classification", "completed"))
      call ignore(httk_workflow_runlog_note("VASP completed"))
      call ignore(httk_workflow_advance("publish"))
    else
      write (message, '(A,I0,A)') "VASP did not complete (status ", run_status, ")"
      call ignore(httk_workflow_fail("vasp.failed", trim(message)))
    end if
    code = 0
  end function

  ! Build the tail httk_workflow_run forwards: the fixed options, then "--", then
  ! the configured command word-split on whitespace, as the Bash runner leaves it
  ! unquoted for the shell.
  subroutine build_run_args(timeout, command, args)
    character(len=*), intent(in) :: timeout, command
    character(len=ARG_WIDTH), allocatable, intent(out) :: args(:)
    integer, parameter :: FIXED = 5
    integer :: i, n, start, ntok
    integer, allocatable :: tstart(:), tend(:)
    logical :: in_token
    ! One pass to find the token boundaries of the command.
    n = len_trim(command)
    allocate (tstart(n), tend(n))
    ntok = 0
    in_token = .false.
    start = 0
    do i = 1, n
      if (is_space(command(i:i))) then
        if (in_token) then
          ntok = ntok + 1
          tstart(ntok) = start
          tend(ntok) = i - 1
          in_token = .false.
        end if
      else
        if (.not. in_token) then
          start = i
          in_token = .true.
        end if
      end if
    end do
    if (in_token) then
      ntok = ntok + 1
      tstart(ntok) = start
      tend(ntok) = n
    end if
    allocate (args(FIXED + ntok))
    args(1) = "--timeout"
    args(2) = timeout
    args(3) = "--report"
    args(4) = "vasp-run-report.json"
    args(5) = "--"
    do i = 1, ntok
      ! Loud failure rather than silent truncation: a token wider than the fixed
      ! ARG_WIDTH field would be clipped, running the wrong command.
      if (tend(i) - tstart(i) + 1 > ARG_WIDTH) then
        write (error_unit, '(A,I0,A)') "relax: a command token exceeds ARG_WIDTH (", ARG_WIDTH, ")"
        call httk_workflow_exit(2)
      end if
      args(FIXED + i) = command(tstart(i):tend(i))
    end do
  end subroutine

  logical function is_space(c)
    character, intent(in) :: c
    is_space = c == " " .or. iachar(c) == 9
  end function

  function step_publish() result(code) bind(c)
    integer(c_int) :: code
    character(len=:), allocatable :: prefix, data_dir, operation
    integer :: i
    logical :: to_data
    call httk_workflow_parameter("data_prefix", prefix, "vasp")
    if (.not. allocated(prefix)) prefix = "vasp"
    data_dir = env("HTTK_WORKFLOW_DATA_DIR", "")
    to_data = len_trim(data_dir) > 0
    do i = 1, size(COLLECT)
      if (.not. file_exists(trim(COLLECT(i)))) cycle
      if (to_data) then
        call httk_workflow_put(trim(COLLECT(i)), trim(prefix)//"/"//trim(COLLECT(i)), operation)
      end if
    end do
    if (to_data) then
      call ignore(httk_workflow_runlog_note("published to transactional data"))
    else
      call ignore(httk_workflow_runlog_note("kept the result in the workdir"))
    end if
    call ignore(httk_workflow_succeed())
    code = 0
  end function

end module relax_steps

program relax

  use, intrinsic :: iso_c_binding, only: c_funloc
  use httk_workflow
  use relax_steps

  implicit none

  if (httk_workflow_runner("httk.vasp.relax-fortran", &
        [character(len=8) :: "prepare", "run", "publish"], &
        [c_funloc(step_prepare), c_funloc(step_run), c_funloc(step_publish)]) /= HTTK_WORKFLOW_OK) then
    call httk_workflow_exit(2)
  end if
  call httk_workflow_exit(httk_workflow_main())

end program relax
