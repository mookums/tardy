.global _tardy_swap_frame
.global tardy_swap_frame 

_tardy_swap_frame:
tardy_swap_frame:
      stp lr, fp, [sp, #-20*8]!
      stp d8, d9, [sp, #2*8]
      stp d10, d11, [sp, #4*8]
      stp d12, d13, [sp, #6*8]
      stp d14, d15, [sp, #8*8]
      stp x19, x20, [sp, #10*8]
      stp x21, x22, [sp, #12*8]
      stp x23, x24, [sp, #14*8]
      stp x25, x26, [sp, #16*8]
      stp x27, x28, [sp, #18*8]

      mov x9, sp
      str x9, [x0]
      ldr x9, [x1]
      mov sp, x9

      ldp x27, x28, [sp, #18*8]
      ldp x25, x26, [sp, #16*8]
      ldp x23, x24, [sp, #14*8]
      ldp x21, x22, [sp, #12*8]
      ldp x19, x20, [sp, #10*8]
      ldp d14, d15, [sp, #8*8]
      ldp d12, d13, [sp, #6*8]
      ldp d10, d11, [sp, #4*8]
      ldp d8, d9, [sp, #2*8]
      ldp lr, fp, [sp], #20*8
      
      ret
