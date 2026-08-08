! Native httk-workflow modern-Fortran authoring SDK.
!
! This module is a thin, idiomatic Fortran skin over the native C SDK in
! ../c/httk_workflow.{h,c}.  It does NOT reimplement the bridge protocol: every
! verb is a bind(c) call into the C library, so a Fortran runner and a C, Bash,
! or Python runner publish the same bytes.  The C half owns registration,
! dispatch, and the process exit status; this module marshals Fortran strings
! and procedure pointers across the C ABI and copies C-allocated results into
! allocatable Fortran strings, freeing each exactly once.
!
! Build it beside the C source, compiling the two languages separately (the
! Fortran standard flag is not valid for C):
!
!     cc       -std=c99   -c    ../c/httk_workflow.c        -o httk_workflow_c.o
!     gfortran -std=f2008 -c    httk_workflow.f90
!     gfortran -std=f2008       runner.f90 httk_workflow.o httk_workflow_c.o -o runner
!
! A runner declares its workflow and its complete step set once, implements one
! step per declared name as a `bind(c)` `integer(c_int)` function that RETURNS
! (0 when the step ended, whether or not it published; nonzero when it could not
! complete), and ends by delegating to httk_workflow_main:
!
!     module my_steps
!       use, intrinsic :: iso_c_binding, only: c_int
!       use httk_workflow
!       implicit none
!     contains
!       function step_prepare() result(code) bind(c)
!         integer(c_int) :: code
!         call ignore(httk_workflow_advance("run"))
!         code = 0
!       end function
!     end module
!
!     program main
!       use, intrinsic :: iso_c_binding, only: c_funloc
!       use httk_workflow
!       use my_steps
!       implicit none
!       if (httk_workflow_runner("my.workflow", &
!               [character(len=16) :: "prepare", "run"], &
!               [c_funloc(step_prepare), c_funloc(step_run)]) /= HTTK_WORKFLOW_OK) &
!           call exit(2)
!       call exit(httk_workflow_main())
!     end program
!
! The step handler is a `bind(c)` function returning `integer(c_int)` rather than
! the plainer Fortran subroutine, because the C dispatcher calls it through an
! `int (*)(void)` pointer: c_funloc requires an interoperable target, and reading
! the return value of a `void` procedure through that pointer would be undefined.
! See docs/sdks/native_fortran_api.md for the rationale and the string-ownership rules.

module httk_workflow

  use, intrinsic :: iso_c_binding

  implicit none
  private

  ! --- Re-exported bridge exit-status discipline, identical to the C SDK. -----
  integer, parameter, public :: HTTK_WORKFLOW_OK = 0
  integer, parameter, public :: HTTK_WORKFLOW_ABSENT = 1
  integer, parameter, public :: HTTK_WORKFLOW_REFUSED = 2
  ! The version of this native library, mirroring HTTK_WORKFLOW_C_API_VERSION.
  integer, parameter, public :: HTTK_WORKFLOW_FORTRAN_API_VERSION = 2

  ! The signature every step handler must have: a `bind(c)` function of no
  ! arguments returning `integer(c_int)`, whose address is handed to the C
  ! dispatcher with c_funloc.  Provided for documentation; c_funloc accepts any
  ! interoperable procedure, so a handler need not name this interface.
  public :: httk_workflow_step_fn
  abstract interface
    function httk_workflow_step_fn() result(code) bind(c)
      import :: c_int
      integer(c_int) :: code
    end function
  end interface

  ! --- Public authoring surface. ---------------------------------------------
  public :: httk_workflow_runner, httk_workflow_main, httk_workflow_describe
  public :: httk_workflow_invoke
  public :: httk_workflow_context, httk_workflow_parameter, httk_workflow_setting
  public :: httk_workflow_environment, httk_workflow_state_get
  public :: httk_workflow_declaration, httk_workflow_children, httk_workflow_child
  public :: httk_workflow_state_set, httk_workflow_state_delete, httk_workflow_state_merge
  public :: httk_workflow_declare, httk_workflow_runlog_note, httk_workflow_runlog_headline
  public :: httk_workflow_runlog_append, httk_workflow_log
  public :: httk_workflow_put, httk_workflow_remove, httk_workflow_spawn
  public :: httk_workflow_advance, httk_workflow_gather, httk_workflow_succeed
  public :: httk_workflow_fail, httk_workflow_retry, httk_workflow_pause
  public :: httk_workflow_batch, httk_workflow_job_prepare, httk_workflow_workdir_apply
  public :: httk_workflow_run, httk_calc, httk_template_render
  public :: httk_compress, httk_decompress
  public :: ignore, httk_workflow_exit

  ! --- The C struct httk_workflow_step, laid out for the C ABI. ---------------
  type, bind(c) :: c_step
    type(c_ptr) :: name
    type(c_funptr) :: handler
  end type

  ! The registration the C library keeps a pointer into; it must outlive the
  ! process, so this SDK owns it as saved module state (allocated exactly once).
  character(kind=c_char), allocatable, target, save :: g_workflow(:)
  character(kind=c_char), allocatable, target, save :: g_names(:)
  type(c_step), allocatable, target, save :: g_steps(:)

  ! --- The private bind(c) interface over every exported C function. ----------
  ! Every `const char *` in-parameter is bound as `type(c_ptr), value` so a NULL
  ! (an absent optional) and a real string are marshalled the same way; every
  ! malloc'd `char *` return is bound as `type(c_ptr)` and copied out with take().
  interface

    function c_runner(workflow, steps, count) bind(c, name="httk_workflow_runner") result(rc)
      import :: c_ptr, c_size_t, c_int
      type(c_ptr), value :: workflow, steps
      integer(c_size_t), value :: count
      integer(c_int) :: rc
    end function

    function c_main(argc, argv) bind(c, name="httk_workflow_main") result(rc)
      import :: c_int, c_ptr
      integer(c_int), value :: argc
      type(c_ptr), value :: argv
      integer(c_int) :: rc
    end function

    subroutine c_describe() bind(c, name="httk_workflow_describe")
    end subroutine

    function c_invoke(out, argv) bind(c, name="httk_workflow_invoke") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: out, argv
      integer(c_int) :: rc
    end function

    function c_context(field, status) bind(c, name="httk_workflow_context") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: field
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_named(name, fallback, status) bind(c, name="httk_workflow_parameter") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: name, fallback
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_setting(name, fallback, status) bind(c, name="httk_workflow_setting") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: name, fallback
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_environment(name, fallback, status) bind(c, name="httk_workflow_environment") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: name, fallback
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_state_get(name, status) bind(c, name="httk_workflow_state_get") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: name
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_declaration(name, status) bind(c, name="httk_workflow_declaration") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: name
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_children(selection, status) bind(c, name="httk_workflow_children") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: selection
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_child(label, field, status) bind(c, name="httk_workflow_child") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: label, field
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_state_set(name, value) bind(c, name="httk_workflow_state_set") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: name, value
      integer(c_int) :: rc
    end function

    function c_state_delete(name) bind(c, name="httk_workflow_state_delete") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: name
      integer(c_int) :: rc
    end function

    function c_state_merge(assignments) bind(c, name="httk_workflow_state_merge") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: assignments
      integer(c_int) :: rc
    end function

    function c_declare(name, document_file) bind(c, name="httk_workflow_declare") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: name, document_file
      integer(c_int) :: rc
    end function

    function c_runlog_note(message) bind(c, name="httk_workflow_runlog_note") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: message
      integer(c_int) :: rc
    end function

    function c_runlog_headline(message) bind(c, name="httk_workflow_runlog_headline") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: message
      integer(c_int) :: rc
    end function

    function c_runlog_append(message, files) bind(c, name="httk_workflow_runlog_append") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: message, files
      integer(c_int) :: rc
    end function

    function c_log(level, message) bind(c, name="httk_workflow_log") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: level, message
      integer(c_int) :: rc
    end function

    function c_put(source, destination, status) bind(c, name="httk_workflow_put") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: source, destination
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_remove(destination, missing_ok, status) bind(c, name="httk_workflow_remove") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: destination
      integer(c_int), value :: missing_ok
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_spawn(label, args, status) bind(c, name="httk_workflow_spawn") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: label, args
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_advance(next_step, args) bind(c, name="httk_workflow_advance") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: next_step, args
      integer(c_int) :: rc
    end function

    function c_gather(next_step, args) bind(c, name="httk_workflow_gather") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: next_step, args
      integer(c_int) :: rc
    end function

    function c_succeed() bind(c, name="httk_workflow_succeed") result(rc)
      import :: c_int
      integer(c_int) :: rc
    end function

    function c_fail(code, message, args) bind(c, name="httk_workflow_fail") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: code, message, args
      integer(c_int) :: rc
    end function

    function c_retry(reason) bind(c, name="httk_workflow_retry") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: reason
      integer(c_int) :: rc
    end function

    function c_pause(reason) bind(c, name="httk_workflow_pause") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: reason
      integer(c_int) :: rc
    end function

    function c_batch() bind(c, name="httk_workflow_batch") result(rc)
      import :: c_int
      integer(c_int) :: rc
    end function

    function c_job_prepare(destination, spec_file, status) bind(c, name="httk_workflow_job_prepare") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: destination, spec_file
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_workdir_apply(spec_file, status) bind(c, name="httk_workflow_workdir_apply") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: spec_file
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_run(args) bind(c, name="httk_workflow_run") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: args
      integer(c_int) :: rc
    end function

    function c_calc(expression, status) bind(c, name="httk_calc") result(p)
      import :: c_ptr, c_int
      type(c_ptr), value :: expression
      integer(c_int), intent(out) :: status
      type(c_ptr) :: p
    end function

    function c_template(template_file, output, values_file) bind(c, name="httk_template_render") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: template_file, output, values_file
      integer(c_int) :: rc
    end function

    function c_compress(args) bind(c, name="httk_compress") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: args
      integer(c_int) :: rc
    end function

    function c_decompress(args) bind(c, name="httk_decompress") result(rc)
      import :: c_ptr, c_int
      type(c_ptr), value :: args
      integer(c_int) :: rc
    end function

    ! libc, for owning the malloc'd string returns exactly once.
    function c_strlen(p) bind(c, name="strlen") result(n)
      import :: c_ptr, c_size_t
      type(c_ptr), value :: p
      integer(c_size_t) :: n
    end function

    subroutine c_free(p) bind(c, name="free")
      import :: c_ptr
      type(c_ptr), value :: p
    end subroutine

    subroutine c_exit(status) bind(c, name="exit")
      import :: c_int
      integer(c_int), value :: status
    end subroutine

  end interface

contains

  ! --- String marshalling across the C boundary. -----------------------------

  ! One NUL-terminated C copy of a Fortran string, trailing blanks stripped
  ! (a blank-padded scalar cannot express a meaningful trailing space anyway).
  pure function cstr(s) result(buf)
    character(len=*), intent(in) :: s
    character(kind=c_char), allocatable :: buf(:)
    integer :: i, n
    n = len_trim(s)
    allocate (buf(n + 1))
    do i = 1, n
      buf(i) = s(i:i)
    end do
    buf(n + 1) = c_null_char
  end function

  ! Copy a malloc'd C string into an allocatable Fortran string and free it.
  ! This is a subroutine, not a function, on purpose: `str` is `intent(out)`, so
  ! it starts deallocated, and an unassociated pointer (an absent answer) leaves
  ! it *unallocated* with defined semantics -- whereas an unallocated function
  ! result is undefined to assign from, and gfortran yields an indistinguishable
  ! zero-length string, collapsing "absent" into "empty". A caller therefore
  ! tells the two apart with `allocated(str)` or the accompanying status.
  subroutine take(p, str)
    type(c_ptr), intent(in) :: p
    character(len=:), allocatable, intent(out) :: str
    character(kind=c_char), pointer :: chars(:)
    integer(c_size_t) :: n
    integer :: i
    if (.not. c_associated(p)) return
    n = c_strlen(p)
    call c_f_pointer(p, chars, [n])
    allocate (character(len=int(n)) :: str)
    do i = 1, int(n)
      str(i:i) = chars(i)
    end do
    call c_free(p)
  end subroutine

  ! Marshal a Fortran string array into a NUL-terminated C `char *[]`, packing
  ! the copies into one flat buffer and pointing one row of `ptrs` at each; both
  ! outputs must outlive the C call, so callers hold them as locals.
  subroutine cstr_array(items, flat, ptrs)
    character(len=*), intent(in) :: items(:)
    character(kind=c_char), allocatable, target, intent(out) :: flat(:)
    type(c_ptr), allocatable, target, intent(out) :: ptrs(:)
    integer :: j, k, off, total
    total = 0
    do j = 1, size(items)
      total = total + len_trim(items(j)) + 1
    end do
    allocate (flat(max(total, 1)))
    allocate (ptrs(size(items) + 1))
    off = 0
    do j = 1, size(items)
      ptrs(j) = c_loc(flat(off + 1))
      do k = 1, len_trim(items(j))
        flat(off + k) = items(j) (k:k)
      end do
      off = off + len_trim(items(j))
      flat(off + 1) = c_null_char
      off = off + 1
    end do
    ptrs(size(items) + 1) = c_null_ptr
  end subroutine

  ! Swallow the status a step body does not inspect, so `call ignore(verb(...))`
  ! reads cleanly for the frequent publish-and-ignore case.
  subroutine ignore(status)
    integer, intent(in) :: status
    if (status < -huge(status)) continue  ! reference the argument; never true
  end subroutine

  ! --- Registration, dispatch, and description. ------------------------------

  ! Declare the workflow and the complete step set, then hand the C library the
  ! registration it keeps for the process.  `names` are the step names and
  ! `handlers` their c_funloc addresses, in the same order.  Returns
  ! HTTK_WORKFLOW_OK, or HTTK_WORKFLOW_REFUSED on bad usage (mismatched lengths,
  ! or whatever the C validator rejects).  Under HTTK_WORKFLOW_DESCRIBE=1 the C
  ! library prints the description and exits before returning.
  function httk_workflow_runner(workflow, names, handlers) result(rc)
    character(len=*), intent(in) :: workflow
    character(len=*), intent(in) :: names(:)
    type(c_funptr), intent(in) :: handlers(:)
    integer :: rc
    integer :: i, k, off, total
    if (size(names) /= size(handlers) .or. size(names) == 0) then
      rc = HTTK_WORKFLOW_REFUSED
      return
    end if
    ! Re-registration replaces the previous set, as the Bash and C SDKs do; the
    ! saved arrays are deallocated first so a second call does not abort.
    g_workflow = cstr(workflow)
    total = 0
    do i = 1, size(names)
      total = total + len_trim(names(i)) + 1
    end do
    if (allocated(g_names)) deallocate (g_names)
    if (allocated(g_steps)) deallocate (g_steps)
    allocate (g_names(max(total, 1)))
    allocate (g_steps(size(names)))
    off = 0
    do i = 1, size(names)
      g_steps(i)%name = c_loc(g_names(off + 1))
      do k = 1, len_trim(names(i))
        g_names(off + k) = names(i) (k:k)
      end do
      off = off + len_trim(names(i))
      g_names(off + 1) = c_null_char
      off = off + 1
      g_steps(i)%handler = handlers(i)
    end do
    rc = int(c_runner(c_loc(g_workflow), c_loc(g_steps), int(size(names), c_size_t)))
  end function

  ! Dispatch the step the manager asked for and turn its ending into exactly one
  ! outcome, forwarding this process's command line so `--describe` is honoured.
  ! The C library owns the returned exit status.
  function httk_workflow_main() result(rc)
    integer :: rc
    integer :: argc, i, k, l, off, total
    character(len=:), allocatable :: arg
    character(kind=c_char), allocatable, target :: flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    argc = command_argument_count()
    ! argv[0] is "runner" (the C dispatcher ignores it); argv[1:] are the args.
    total = len("runner") + 1
    do i = 1, argc
      call get_command_argument(i, length=l)
      total = total + l + 1
    end do
    allocate (flat(total))
    allocate (ptrs(argc + 2))
    ptrs(1) = c_loc(flat(1))
    do k = 1, len("runner")
      flat(k) = "runner" (k:k)
    end do
    flat(len("runner") + 1) = c_null_char
    off = len("runner") + 1
    do i = 1, argc
      call get_command_argument(i, length=l)
      allocate (character(len=l) :: arg)
      call get_command_argument(i, value=arg)
      ptrs(i + 1) = c_loc(flat(off + 1))
      do k = 1, l
        flat(off + k) = arg(k:k)
      end do
      off = off + l
      flat(off + 1) = c_null_char
      off = off + 1
      deallocate (arg)
    end do
    ptrs(argc + 2) = c_null_ptr
    rc = int(c_main(int(argc + 1, c_int), c_loc(ptrs(1))))
  end function

  ! Print this runner's machine-readable description to stdout.
  subroutine httk_workflow_describe()
    call c_describe()
  end subroutine

  ! Terminate the process with `status` as its exit code, the way a C runner
  ! `return`s the value of httk_workflow_main from main.  A Fortran program end
  ! always exits 0, so a runner needs this to propagate the status the C
  ! dispatcher owns.  Never returns.  (libc exit, so the exit code is exact and
  ! carries no STOP diagnostic, and the SDK stays within Fortran 2008.)
  subroutine httk_workflow_exit(status)
    integer, intent(in) :: status
    call c_exit(int(status, c_int))
  end subroutine

  ! The escape hatch: run one bridge subcommand named by `argv` (the subcommand
  ! and its arguments).  When `output` is present the subcommand's stdout is
  ! captured into it; otherwise stdout and stdin are inherited.  Returns the
  ! bridge exit status.
  function httk_workflow_invoke(argv, output) result(rc)
    character(len=*), intent(in) :: argv(:)
    character(len=:), allocatable, intent(out), optional :: output
    integer :: rc
    character(kind=c_char), allocatable, target :: flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    type(c_ptr), target :: captured
    call cstr_array(argv, flat, ptrs)
    if (present(output)) then
      captured = c_null_ptr
      rc = int(c_invoke(c_loc(captured), c_loc(ptrs(1))))
      call take(captured, output)
    else
      rc = int(c_invoke(c_null_ptr, c_loc(ptrs(1))))
    end if
  end function

  ! --- What a step reads. -----------------------------------------------------
  !
  ! Every read is a subroutine whose `value` is `intent(out), allocatable`: an
  ! absent answer (the bridge returned nothing) leaves `value` UNALLOCATED, which
  ! `allocated(value)` and the optional `status` both report, while a legitimate
  ! empty string arrives ALLOCATED with length zero. That distinction cannot
  ! survive a function result, which is why these are not functions.

  ! The attempt context, or one field of it when `field` is present.
  subroutine httk_workflow_context(value, field, status)
    character(len=:), allocatable, intent(out) :: value
    character(len=*), intent(in), optional :: field
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bfield(:)
    integer(c_int) :: st
    if (present(field)) then
      bfield = cstr(field)
      call take(c_context(c_loc(bfield), st), value)
    else
      call take(c_context(c_null_ptr, st), value)
    end if
    if (present(status)) status = int(st)
  end subroutine

  ! One member of the job's parameters object, with an optional default.
  subroutine httk_workflow_parameter(name, value, fallback, status)
    character(len=*), intent(in) :: name
    character(len=:), allocatable, intent(out) :: value
    character(len=*), intent(in), optional :: fallback
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bname(:), bfall(:)
    type(c_ptr) :: pfall
    integer(c_int) :: st
    bname = cstr(name)
    if (present(fallback)) then
      bfall = cstr(fallback)
      pfall = c_loc(bfall)
    else
      pfall = c_null_ptr
    end if
    call take(c_named(c_loc(bname), pfall, st), value)
    if (present(status)) status = int(st)
  end subroutine

  ! One resolved application setting, with an optional default.
  subroutine httk_workflow_setting(name, value, fallback, status)
    character(len=*), intent(in) :: name
    character(len=:), allocatable, intent(out) :: value
    character(len=*), intent(in), optional :: fallback
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bname(:), bfall(:)
    type(c_ptr) :: pfall
    integer(c_int) :: st
    bname = cstr(name)
    if (present(fallback)) then
      bfall = cstr(fallback)
      pfall = c_loc(bfall)
    else
      pfall = c_null_ptr
    end if
    call take(c_setting(c_loc(bname), pfall, st), value)
    if (present(status)) status = int(st)
  end subroutine

  ! One declared workflow environment value, with an optional default.
  subroutine httk_workflow_environment(name, value, fallback, status)
    character(len=*), intent(in) :: name
    character(len=:), allocatable, intent(out) :: value
    character(len=*), intent(in), optional :: fallback
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bname(:), bfall(:)
    type(c_ptr) :: pfall
    integer(c_int) :: st
    bname = cstr(name)
    if (present(fallback)) then
      bfall = cstr(fallback)
      pfall = c_loc(bfall)
    else
      pfall = c_null_ptr
    end if
    call take(c_environment(c_loc(bname), pfall, st), value)
    if (present(status)) status = int(st)
  end subroutine

  ! One key of the job's JSON state; absent (status 1, unallocated) when unset.
  subroutine httk_workflow_state_get(name, value, status)
    character(len=*), intent(in) :: name
    character(len=:), allocatable, intent(out) :: value
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bname(:)
    integer(c_int) :: st
    bname = cstr(name)
    call take(c_state_get(c_loc(bname), st), value)
    if (present(status)) status = int(st)
  end subroutine

  ! One workflow declaration: the observed document, else the declared one.
  subroutine httk_workflow_declaration(name, value, status)
    character(len=*), intent(in) :: name
    character(len=:), allocatable, intent(out) :: value
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bname(:)
    integer(c_int) :: st
    bname = cstr(name)
    call take(c_declaration(c_loc(bname), st), value)
    if (present(status)) status = int(st)
  end subroutine

  ! The observed children as tab-separated rows; `selection` is one of
  ! "--all", "--succeeded", or "--failed", or absent for the default.
  subroutine httk_workflow_children(value, selection, status)
    character(len=:), allocatable, intent(out) :: value
    character(len=*), intent(in), optional :: selection
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bsel(:)
    type(c_ptr) :: psel
    integer(c_int) :: st
    if (present(selection)) then
      bsel = cstr(selection)
      psel = c_loc(bsel)
    else
      psel = c_null_ptr
    end if
    call take(c_children(psel, st), value)
    if (present(status)) status = int(st)
  end subroutine

  ! One field of one observed child by label.
  subroutine httk_workflow_child(label, field, value, status)
    character(len=*), intent(in) :: label, field
    character(len=:), allocatable, intent(out) :: value
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: blabel(:), bfield(:)
    integer(c_int) :: st
    blabel = cstr(label)
    bfield = cstr(field)
    call take(c_child(c_loc(blabel), c_loc(bfield), st), value)
    if (present(status)) status = int(st)
  end subroutine

  ! --- Job state. -------------------------------------------------------------

  function httk_workflow_state_set(name, value) result(rc)
    character(len=*), intent(in) :: name, value
    integer :: rc
    character(kind=c_char), allocatable, target :: bname(:), bvalue(:)
    bname = cstr(name)
    bvalue = cstr(value)
    rc = int(c_state_set(c_loc(bname), c_loc(bvalue)))
  end function

  function httk_workflow_state_delete(name) result(rc)
    character(len=*), intent(in) :: name
    integer :: rc
    character(kind=c_char), allocatable, target :: bname(:)
    bname = cstr(name)
    rc = int(c_state_delete(c_loc(bname)))
  end function

  ! Several NAME=VALUE assignments in one atomic replace.
  function httk_workflow_state_merge(assignments) result(rc)
    character(len=*), intent(in) :: assignments(:)
    integer :: rc
    character(kind=c_char), allocatable, target :: flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    call cstr_array(assignments, flat, ptrs)
    rc = int(c_state_merge(c_loc(ptrs(1))))
  end function

  ! --- Declarations and the run log. ------------------------------------------

  function httk_workflow_declare(name, document_file) result(rc)
    character(len=*), intent(in) :: name, document_file
    integer :: rc
    character(kind=c_char), allocatable, target :: bname(:), bfile(:)
    bname = cstr(name)
    bfile = cstr(document_file)
    rc = int(c_declare(c_loc(bname), c_loc(bfile)))
  end function

  function httk_workflow_runlog_note(message) result(rc)
    character(len=*), intent(in) :: message
    integer :: rc
    character(kind=c_char), allocatable, target :: b(:)
    b = cstr(message)
    rc = int(c_runlog_note(c_loc(b)))
  end function

  function httk_workflow_runlog_headline(message) result(rc)
    character(len=*), intent(in) :: message
    integer :: rc
    character(kind=c_char), allocatable, target :: b(:)
    b = cstr(message)
    rc = int(c_runlog_headline(c_loc(b)))
  end function

  ! One event with whole files (by content) attached.
  function httk_workflow_runlog_append(message, files) result(rc)
    character(len=*), intent(in) :: message
    character(len=*), intent(in), optional :: files(:)
    integer :: rc
    character(kind=c_char), allocatable, target :: b(:), flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    type(c_ptr) :: pfiles
    b = cstr(message)
    if (present(files)) then
      call cstr_array(files, flat, ptrs)
      pfiles = c_loc(ptrs(1))
    else
      pfiles = c_null_ptr
    end if
    rc = int(c_runlog_append(c_loc(b), pfiles))
  end function

  ! One timestamped "LEVEL MESSAGE" line to stderr; local, no bridge.
  function httk_workflow_log(level, message) result(rc)
    character(len=*), intent(in) :: level, message
    integer :: rc
    character(kind=c_char), allocatable, target :: blevel(:), bmessage(:)
    blevel = cstr(level)
    bmessage = cstr(message)
    rc = int(c_log(c_loc(blevel), c_loc(bmessage)))
  end function

  ! --- Transactional data. ----------------------------------------------------

  ! Stage one file or tree into the job's data; `operation` is the operation id.
  subroutine httk_workflow_put(source, destination, operation, status)
    character(len=*), intent(in) :: source, destination
    character(len=:), allocatable, intent(out) :: operation
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bsource(:), bdest(:)
    integer(c_int) :: st
    bsource = cstr(source)
    bdest = cstr(destination)
    call take(c_put(c_loc(bsource), c_loc(bdest), st), operation)
    if (present(status)) status = int(st)
  end subroutine

  ! Stage one removal from the job's data; `operation` is the operation id.
  subroutine httk_workflow_remove(destination, operation, missing_ok, status)
    character(len=*), intent(in) :: destination
    character(len=:), allocatable, intent(out) :: operation
    logical, intent(in), optional :: missing_ok
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bdest(:)
    integer(c_int) :: st, flag
    bdest = cstr(destination)
    flag = 0
    if (present(missing_ok)) then
      if (missing_ok) flag = 1
    end if
    call take(c_remove(c_loc(bdest), flag, st), operation)
    if (present(status)) status = int(st)
  end subroutine

  ! Register one child under a unique label; `args` carries the child options,
  ! and `job_key` receives the child's job key.
  subroutine httk_workflow_spawn(label, job_key, args, status)
    character(len=*), intent(in) :: label
    character(len=:), allocatable, intent(out) :: job_key
    character(len=*), intent(in), optional :: args(:)
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: blabel(:), flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    type(c_ptr) :: pargs
    integer(c_int) :: st
    blabel = cstr(label)
    if (present(args)) then
      call cstr_array(args, flat, ptrs)
      pargs = c_loc(ptrs(1))
    else
      pargs = c_null_ptr
    end if
    call take(c_spawn(c_loc(blabel), pargs, st), job_key)
    if (present(status)) status = int(st)
  end subroutine

  ! --- What a step publishes (exactly one per attempt). -----------------------

  function httk_workflow_advance(next_step, args) result(rc)
    character(len=*), intent(in) :: next_step
    character(len=*), intent(in), optional :: args(:)
    integer :: rc
    character(kind=c_char), allocatable, target :: bnext(:), flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    type(c_ptr) :: pargs
    bnext = cstr(next_step)
    if (present(args)) then
      call cstr_array(args, flat, ptrs)
      pargs = c_loc(ptrs(1))
    else
      pargs = c_null_ptr
    end if
    rc = int(c_advance(c_loc(bnext), pargs))
  end function

  function httk_workflow_gather(next_step, args) result(rc)
    character(len=*), intent(in) :: next_step
    character(len=*), intent(in), optional :: args(:)
    integer :: rc
    character(kind=c_char), allocatable, target :: bnext(:), flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    type(c_ptr) :: pargs
    bnext = cstr(next_step)
    if (present(args)) then
      call cstr_array(args, flat, ptrs)
      pargs = c_loc(ptrs(1))
    else
      pargs = c_null_ptr
    end if
    rc = int(c_gather(c_loc(bnext), pargs))
  end function

  function httk_workflow_succeed() result(rc)
    integer :: rc
    rc = int(c_succeed())
  end function

  ! Publish a structured terminal failure; `args` may carry --details,
  ! --retryable, and --priority.
  function httk_workflow_fail(code, message, args) result(rc)
    character(len=*), intent(in) :: code, message
    character(len=*), intent(in), optional :: args(:)
    integer :: rc
    character(kind=c_char), allocatable, target :: bcode(:), bmessage(:), flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    type(c_ptr) :: pargs
    bcode = cstr(code)
    bmessage = cstr(message)
    if (present(args)) then
      call cstr_array(args, flat, ptrs)
      pargs = c_loc(ptrs(1))
    else
      pargs = c_null_ptr
    end if
    rc = int(c_fail(c_loc(bcode), c_loc(bmessage), pargs))
  end function

  function httk_workflow_retry(reason) result(rc)
    character(len=*), intent(in) :: reason
    integer :: rc
    character(kind=c_char), allocatable, target :: b(:)
    b = cstr(reason)
    rc = int(c_retry(c_loc(b)))
  end function

  function httk_workflow_pause(reason) result(rc)
    character(len=*), intent(in) :: reason
    integer :: rc
    character(kind=c_char), allocatable, target :: b(:)
    b = cstr(reason)
    rc = int(c_pause(c_loc(b)))
  end function

  ! --- Batch, payload and workdir, utilities. ---------------------------------

  ! Run several bridge commands, one per stdin line, in one interpreter start.
  function httk_workflow_batch() result(rc)
    integer :: rc
    rc = int(c_batch())
  end function

  ! Write job.json into a prepared payload from a JobSpec file; `job` is its JSON.
  subroutine httk_workflow_job_prepare(destination, spec_file, job, status)
    character(len=*), intent(in) :: destination, spec_file
    character(len=:), allocatable, intent(out) :: job
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bdest(:), bspec(:)
    integer(c_int) :: st
    bdest = cstr(destination)
    bspec = cstr(spec_file)
    call take(c_job_prepare(c_loc(bdest), c_loc(bspec), st), job)
    if (present(status)) status = int(st)
  end subroutine

  ! Apply a replayable batch of workdir changes from a spec file; `id` is its id.
  subroutine httk_workflow_workdir_apply(spec_file, id, status)
    character(len=*), intent(in) :: spec_file
    character(len=:), allocatable, intent(out) :: id
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: bspec(:)
    integer(c_int) :: st
    bspec = cstr(spec_file)
    call take(c_workdir_apply(c_loc(bspec), st), id)
    if (present(status)) status = int(st)
  end subroutine

  ! Run an argv array under supervision; `args` is the whole tail the C function
  ! forwards (options, then "--", then the argv).  Returns the classified status.
  function httk_workflow_run(args) result(rc)
    character(len=*), intent(in) :: args(:)
    integer :: rc
    character(kind=c_char), allocatable, target :: flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    call cstr_array(args, flat, ptrs)
    rc = int(c_run(c_loc(ptrs(1))))
  end function

  ! Evaluate one arithmetic expression without a shell; `value` is its result.
  subroutine httk_calc(expression, value, status)
    character(len=*), intent(in) :: expression
    character(len=:), allocatable, intent(out) :: value
    integer, intent(out), optional :: status
    character(kind=c_char), allocatable, target :: b(:)
    integer(c_int) :: st
    b = cstr(expression)
    call take(c_calc(c_loc(b), st), value)
    if (present(status)) status = int(st)
  end subroutine

  function httk_template_render(template_file, output, values_file) result(rc)
    character(len=*), intent(in) :: template_file, output, values_file
    integer :: rc
    character(kind=c_char), allocatable, target :: btmpl(:), bout(:), bvals(:)
    btmpl = cstr(template_file)
    bout = cstr(output)
    bvals = cstr(values_file)
    rc = int(c_template(c_loc(btmpl), c_loc(bout), c_loc(bvals)))
  end function

  function httk_compress(args) result(rc)
    character(len=*), intent(in) :: args(:)
    integer :: rc
    character(kind=c_char), allocatable, target :: flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    call cstr_array(args, flat, ptrs)
    rc = int(c_compress(c_loc(ptrs(1))))
  end function

  function httk_decompress(args) result(rc)
    character(len=*), intent(in) :: args(:)
    integer :: rc
    character(kind=c_char), allocatable, target :: flat(:)
    type(c_ptr), allocatable, target :: ptrs(:)
    call cstr_array(args, flat, ptrs)
    rc = int(c_decompress(c_loc(ptrs(1))))
  end function

end module httk_workflow
