import { ReactNode } from 'react'
import { FallbackProps } from 'react-error-boundary'

export interface ErrorHandlerType extends FallbackProps {
  componentStack?:ReactNode
}
