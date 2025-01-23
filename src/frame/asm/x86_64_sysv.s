.global _tardy_swap_frame
.global tardy_swap_frame

_tardy_swap_frame:
tardy_swap_frame:
    pushq %rbx
    pushq %rbp
    pushq %r12
    pushq %r13
    pushq %r14
    pushq %r15

    // swap stacks
    movq %rsp, (%rdi)
    movq (%rsi), %rsp

    popq %r15
    popq %r14
    popq %r13
    popq %r12
    popq %rbp
    popq %rbx
    retq
