.global tardy_swap_frame

tardy_swap_frame:
    pushq %gs:0x10
    pushq %gs:0x08

    pushq %rbx
    pushq %rbp
    pushq %rdi
    pushq %rsi
    pushq %r12
    pushq %r13
    pushq %r14
    pushq %r15

    subq $160, %rsp
    movups %xmm6, 0x00(%rsp)
    movups %xmm7, 0x10(%rsp)
    movups %xmm8, 0x20(%rsp)
    movups %xmm9, 0x30(%rsp)
    movups %xmm10, 0x40(%rsp)
    movups %xmm11, 0x50(%rsp)
    movups %xmm12, 0x60(%rsp)
    movups %xmm13, 0x70(%rsp)
    movups %xmm14, 0x80(%rsp)
    movups %xmm15, 0x90(%rsp)

    movq %rsp, (%rcx)
    movq (%rdx), %rsp

    movups 0x00(%rsp), %xmm6
    movups 0x10(%rsp), %xmm7
    movups 0x20(%rsp), %xmm8
    movups 0x30(%rsp), %xmm9
    movups 0x40(%rsp), %xmm10
    movups 0x50(%rsp), %xmm11
    movups 0x60(%rsp), %xmm12
    movups 0x70(%rsp), %xmm13
    movups 0x80(%rsp), %xmm14
    movups 0x90(%rsp), %xmm15
    addq $160, %rsp

    popq %r15
    popq %r14
    popq %r13
    popq %r12
    popq %rsi
    popq %rdi
    popq %rbp
    popq %rbx

    popq %gs:0x08
    popq %gs:0x10

    retq
